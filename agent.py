import os
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd

from crewai import Agent, Task, Crew, Process
from crewai_tools import tool

from langchain_google_genai import ChatGoogleGenerativeAI


REGION = os.getenv("AWS_REGION", "ap-south-1")
LOG_GROUP = os.getenv("LOG_GROUP", "/aws/lambda/app")
SERVICE_NAME = os.getenv("SERVICE_NAME", "checkout")
ECS_CLUSTER = os.getenv("ECS_CLUSTER", "")
CODEDEPLOY_APP = os.getenv("CODEDEPLOY_APP", "")
CODEDEPLOY_DEPLOYMENT_GROUP = os.getenv("CODEDEPLOY_DEPLOYMENT_GROUP", "")

cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
ecs = boto3.client("ecs", region_name=REGION)
codedeploy = boto3.client("codedeploy", region_name=REGION)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _safe_json(x: Any) -> str:
    try:
        return json.dumps(x, default=str, ensure_ascii=False)
    except Exception:
        return str(x)


@tool("cloudwatch_get_metric_timeseries")
def cloudwatch_get_metric_timeseries(
    namespace: str,
    metric_name: str,
    dimensions: List[Dict[str, str]],
    stat: str = "Average",
    period_seconds: int = 60,
    minutes_back: int = 60,
) -> Dict[str, Any]:
    end = _utc_now()
    start = end - timedelta(minutes=minutes_back)

    resp = cw.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start,
        EndTime=end,
        Period=period_seconds,
        Statistics=[stat],
    )

    points = sorted(resp.get("Datapoints", []), key=lambda d: d["Timestamp"])
    series = [
        {"ts": p["Timestamp"].isoformat(), "value": float(p.get(stat, 0.0))}
        for p in points
    ]
    return {
        "namespace": namespace,
        "metric": metric_name,
        "dimensions": dimensions,
        "stat": stat,
        "period_seconds": period_seconds,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "series": series,
    }


@tool("anomaly_detect_spikes_zscore")
def anomaly_detect_spikes_zscore(
    series: List[Dict[str, Any]],
    z_threshold: float = 3.0,
    min_points: int = 20,
) -> Dict[str, Any]:
    if not series or len(series) < min_points:
        return {
            "anomalies": [],
            "reason": f"insufficient_points={len(series) if series else 0}",
        }

    values = np.array([float(p["value"]) for p in series], dtype=float)
    median = np.median(values)
    mad = np.median(np.abs(values - median)) + 1e-9
    z = 0.6745 * (values - median) / mad

    anomalies = []
    for i, zi in enumerate(z):
        if abs(float(zi)) >= float(z_threshold):
            anomalies.append(
                {
                    "ts": series[i]["ts"],
                    "value": float(values[i]),
                    "zscore_robust": float(zi),
                    "direction": "spike" if values[i] > median else "dip",
                }
            )

    window = {
        "median": float(median),
        "mad": float(mad),
        "z_threshold": float(z_threshold),
        "n": int(len(values)),
    }
    return {"window": window, "anomalies": anomalies}


@tool("cloudwatch_logs_insights_query_errors")
def cloudwatch_logs_insights_query_errors(
    log_group: str,
    minutes_back: int = 60,
    limit: int = 50,
) -> Dict[str, Any]:
    end = _utc_now()
    start = end - timedelta(minutes=minutes_back)

    query = r"""
fields @timestamp, @message, @logStream
| filter @message like /ERROR|Exception|timeout|timed out|5\d\d|panic|stack trace/i
| sort @timestamp desc
| limit %d
""" % int(
        limit
    )

    q = logs.start_query(
        logGroupName=log_group,
        startTime=int(start.timestamp()),
        endTime=int(end.timestamp()),
        queryString=query,
        limit=int(limit),
    )
    qid = q["queryId"]

    for _ in range(60):
        res = logs.get_query_results(queryId=qid)
        if res.get("status") in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(1)

    if res.get("status") != "Complete":
        return {
            "status": res.get("status"),
            "results": [],
            "query": query,
            "log_group": log_group,
        }

    parsed = []
    for row in res.get("results", []):
        item = {c["field"]: c.get("value") for c in row}
        parsed.append(item)

    return {
        "status": "Complete",
        "log_group": log_group,
        "query": query,
        "results": parsed,
    }


@tool("cloudwatch_logs_insights_correlate_trace_ids")
def cloudwatch_logs_insights_correlate_trace_ids(
    log_group: str,
    trace_id_keys: List[str] = None,
    minutes_back: int = 60,
    limit: int = 200,
) -> Dict[str, Any]:
    trace_id_keys = trace_id_keys or [
        "traceId",
        "trace_id",
        "correlationId",
        "requestId",
        "x-amzn-trace-id",
    ]

    end = _utc_now()
    start = end - timedelta(minutes=minutes_back)

    key_regex = "|".join([k.replace("-", r"\-") for k in trace_id_keys])
    query = r"""
fields @timestamp, @message, @logStream
| parse @message /("(%s)"\s*:\s*"(?<tid>[^"]+)")/i
| filter ispresent(tid)
| stats count() as hits, latest(@timestamp) as last_seen, values(@logStream) as streams by tid
| sort hits desc
| limit %d
""" % (
        key_regex,
        int(min(limit, 100)),
    )

    q = logs.start_query(
        logGroupName=log_group,
        startTime=int(start.timestamp()),
        endTime=int(end.timestamp()),
        queryString=query,
        limit=int(min(limit, 100)),
    )
    qid = q["queryId"]

    for _ in range(60):
        res = logs.get_query_results(queryId=qid)
        if res.get("status") in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(1)

    if res.get("status") != "Complete":
        return {
            "status": res.get("status"),
            "results": [],
            "query": query,
            "log_group": log_group,
        }

    parsed = []
    for row in res.get("results", []):
        item = {c["field"]: c.get("value") for c in row}
        parsed.append(item)

    return {
        "status": "Complete",
        "log_group": log_group,
        "query": query,
        "results": parsed,
    }


@tool("ecs_get_recent_service_events")
def ecs_get_recent_service_events(
    cluster: str,
    service: str,
    max_events: int = 20,
) -> Dict[str, Any]:
    if not cluster:
        return {"status": "skipped", "reason": "ECS_CLUSTER not set"}

    resp = ecs.describe_services(cluster=cluster, services=[service])
    svcs = resp.get("services", [])
    if not svcs:
        return {"status": "not_found", "cluster": cluster, "service": service}

    events = svcs[0].get("events", [])[: int(max_events)]
    return {
        "status": "ok",
        "cluster": cluster,
        "service": service,
        "events": [
            {"createdAt": e["createdAt"].isoformat(), "message": e.get("message", "")}
            for e in events
        ],
    }


@tool("codedeploy_get_recent_deployments")
def codedeploy_get_recent_deployments(
    application_name: str,
    deployment_group: str,
    max_deployments: int = 10,
) -> Dict[str, Any]:
    if not application_name or not deployment_group:
        return {
            "status": "skipped",
            "reason": "CODEDEPLOY_APP / CODEDEPLOY_DEPLOYMENT_GROUP not set",
        }

    ids = codedeploy.list_deployments(
        applicationName=application_name,
        deploymentGroupName=deployment_group,
        includeOnlyStatuses=[
            "Created",
            "Queued",
            "InProgress",
            "Succeeded",
            "Failed",
            "Stopped",
            "Ready",
        ],
    ).get("deployments", [])[: int(max_deployments)]

    if not ids:
        return {"status": "ok", "deployments": []}

    details = []
    for did in ids:
        d = codedeploy.get_deployment(deploymentId=did).get("deployment", {})
        details.append(
            {
                "deploymentId": did,
                "status": d.get("status"),
                "createTime": (
                    d.get("createTime").isoformat() if d.get("createTime") else None
                ),
                "completeTime": (
                    d.get("completeTime").isoformat() if d.get("completeTime") else None
                ),
                "revision": d.get("revision"),
                "creator": d.get("creator"),
                "description": d.get("description"),
            }
        )

    return {
        "status": "ok",
        "application": application_name,
        "deployment_group": deployment_group,
        "deployments": details,
    }


@dataclass
class IncidentInputs:
    incident_title: str
    suspected_window_minutes: int = 60
    latency_threshold_ms: int = 2000
    log_group: str = LOG_GROUP
    service_name: str = SERVICE_NAME


def build_llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model="gemini-1.5-pro",
        temperature=0.2,
        max_output_tokens=2048,
    )


llm = build_llm()

commander_agent = Agent(
    role="Commander Agent",
    goal=(
        "Orchestrate incident response using a strict loop: DETECT→PLAN→INVESTIGATE→DECIDE→ACT→REPORT. "
        "Synthesize evidence from sub-agents into a precise RCA and a safe action plan with rollback recommendation when justified."
    ),
    backstory="You run a production incident bridge. You demand evidence and time alignment.",
    llm=llm,
    allow_delegation=False,
    verbose=True,
)

metrics_agent = Agent(
    role="Metrics Agent",
    goal="Identify general anomalies (spikes/dips) in CPU, latency, errors, memory leak indicators; return time windows and evidence.",
    backstory="Telemetry analyst. You reason from time series, not vibes.",
    llm=llm,
    tools=[cloudwatch_get_metric_timeseries, anomaly_detect_spikes_zscore],
    allow_delegation=False,
    verbose=True,
)

logs_agent = Agent(
    role="Logs Agent",
    goal="Find concrete log evidence: error clusters, stack traces, timeouts, correlation IDs/trace IDs around anomaly windows.",
    backstory="Forensic log investigator using CloudWatch Logs Insights.",
    llm=llm,
    tools=[
        cloudwatch_logs_insights_query_errors,
        cloudwatch_logs_insights_correlate_trace_ids,
    ],
    allow_delegation=False,
    verbose=True,
)

deploy_agent = Agent(
    role="Deploy Intelligence Agent",
    goal="Detect deploy/config changes correlated with failures using ECS service events and/or CodeDeploy deployments.",
    backstory="Historian of system changes; you care about causality and timestamps.",
    llm=llm,
    tools=[ecs_get_recent_service_events, codedeploy_get_recent_deployments],
    allow_delegation=False,
    verbose=True,
)


def crew_for_incident(inp: IncidentInputs) -> Crew:
    metrics_task = Task(
        description=(
            f"Analyze last {inp.suspected_window_minutes} minutes of metrics for service={inp.service_name}. "
            "Return anomalies using robust z-score detection. "
            "Prioritize: p99 latency, CPUUtilization, MemoryUtilization (or leak proxies), HTTP 5xx / error rate."
            "\n\n"
            "If you lack exact dimensions, propose 2-3 likely dimension sets and run the tool calls anyway."
            "\n\n"
            "Output JSON with keys: anomalies[], proposed_dimensions[], tool_calls[], notes[]."
        ),
        expected_output="JSON evidence: anomalies and what was checked.",
        agent=metrics_agent,
    )

    logs_task = Task(
        description=(
            f"Query CloudWatch Logs Insights for log_group={inp.log_group} last {inp.suspected_window_minutes} minutes. "
            "Extract top error messages, timeouts, stack traces indicators. Also extract trace/correlation IDs frequency. "
            "Return evidence aligned to timestamps."
            "\n\n"
            "Output JSON with keys: error_samples[], top_signatures[], trace_id_clusters[], tool_calls[]."
        ),
        expected_output="JSON evidence: logs errors, signatures, traceId clusters.",
        agent=logs_agent,
    )

    deploy_task = Task(
        description=(
            "Collect change signals in the same time window. "
            f"For ECS: cluster={ECS_CLUSTER or '<unset>'}, service={inp.service_name}. "
            f"For CodeDeploy: app={CODEDEPLOY_APP or '<unset>'}, dg={CODEDEPLOY_DEPLOYMENT_GROUP or '<unset>'}. "
            "Return any deploy/config events and timestamps."
            "\n\n"
            "Output JSON with keys: ecs_events[], deployments[], tool_calls[], notes[]."
        ),
        expected_output="JSON evidence: deploy/config events.",
        agent=deploy_agent,
    )

    commander_task = Task(
        description=(
            "You are the Autonomous Incident Commander. Use the three evidence bundles to produce a final RCA report in Markdown.\n"
            "Hard requirements:\n"
            "1) DETECT: name the primary symptom + first seen time.\n"
            "2) PLAN: list 3 hypotheses ranked by likelihood.\n"
            "3) INVESTIGATE: map evidence to hypotheses; explicitly call out missing data.\n"
            "4) DECIDE: pick most likely root cause with confidence score 0-100 and justification.\n"
            "5) ACT: propose immediate safe actions (rollback/restart/scale) with blast-radius notes.\n"
            "6) REPORT: produce a clean RCA section with Timeline, Impact, Root Cause, Trigger, Resolution, Follow-ups.\n"
            "\n"
            "If deploy correlated within 15–30 minutes before symptom spike, bias to latent configuration bug and recommend rollback.\n"
            "\n"
            "Return ONLY Markdown."
        ),
        expected_output="Markdown RCA report.",
        agent=commander_agent,
        context=[metrics_task, logs_task, deploy_task],
    )

    return Crew(
        agents=[commander_agent, metrics_agent, logs_agent, deploy_agent],
        tasks=[metrics_task, logs_task, deploy_task, commander_task],
        process=Process.sequential,
        verbose=True,
    )


if __name__ == "__main__":
    inp = IncidentInputs(
        incident_title="Checkout latency spike + DB timeouts",
        suspected_window_minutes=60,
        latency_threshold_ms=2000,
        log_group=LOG_GROUP,
        service_name=SERVICE_NAME,
    )

    crew = crew_for_incident(inp)
    result = crew.kickoff(inputs={"incident": _safe_json(inp.__dict__)})
    print(result)
