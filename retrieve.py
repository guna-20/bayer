import boto3
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
REGION = "us-east-1"  # change if needed
LOG_GROUP = "fantastic-formula"
LOG_STREAM = "ip-172-31-19-159"

client = boto3.client("logs", region_name=REGION)

start_time = int(time.time() * 1000)


def tail_cloudwatch(client, log_group, log_stream, minutes=4):
    start_time = int((time.time() - minutes * 60) * 1000)

    resp = client.filter_log_events(
        logGroupName=log_group,
        logStreamNames=[log_stream],
        startTime=start_time,
        interleaved=True,
    )

    logs = []

    for e in resp.get("events", []):
        ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)

        logs.append(
            {
                "timestamp": ts.isoformat(),
                "message": e["message"].rstrip(),
                "raw": e,
            }
        )

    return logs


logs = tail_cloudwatch(client, LOG_GROUP, LOG_STREAM, minutes=2)

for l in logs:
    print(l["timestamp"], l["message"])
