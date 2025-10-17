import uuid
import random
import time
from datetime import datetime, timezone
from google.cloud import bigquery

# --- CONFIG ---
PROJECT_ID = "realtime-attribution-demo"
DATASET = "demo_attribution"
TABLE = "stream_events"
LOCATION = "US"  # change if your dataset is EU, etc.
TOTAL_EVENTS = 12   # 5–20 as per doc
SLEEP_SEC = 0.7     # small delay so you can watch them arrive

# Pools for simple synthetic data
SOURCES = ["google", "direct", "facebook", "email", "bing"]
MEDIUMS = ["cpc", "organic", "referral", "(none)"]
CAMPAIGNS = ["spring_sale", "retargeting", "brand", "(not set)"]
CHANNELS = ["Paid Search", "Organic Search", "Social", "Email", "Direct", "Other"]
EVENTS = ["page_view", "add_to_cart", "purchase"]

def make_row():
    event_name = random.choices(EVENTS, weights=[70, 20, 10], k=1)[0]
    value = round(random.uniform(10, 300), 2) if event_name == "purchase" else None
    now_iso = datetime.now(timezone.utc).isoformat()

    row = {
        "event_id": str(uuid.uuid4()),              # unique ID for dedupe
        "user_pseudo_id": f"user_{random.randint(1000,9999)}",
        "event_name": event_name,
        "source": random.choice(SOURCES),
        "medium": random.choice(MEDIUMS),
        "campaign": random.choice(CAMPAIGNS),
        "channel": random.choice(CHANNELS),
        "conversion_value": value,
        "event_ts": now_iso
    }
    return row

def main():
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    table = client.get_table(table_ref)  # ensures table exists

    for i in range(TOTAL_EVENTS):
        row = make_row()


        # ---- Use a free batch load job instead of streaming insert ----
    job = client.load_table_from_json([row], table)
    job.result()  # wait for completion
    print(f"[{i+1}] ✅ Loaded {row['event_id']}  {row['event_name']}  @{row['event_ts']}")
    time.sleep(SLEEP_SEC)

        # if errors:
        #     print(f"[{i+1}] ❌ Insert error: {errors}")
        # else:
        #     print(f"[{i+1}] ✅ Inserted {row['event_id']}  {row['event_name']}  @{row['event_ts']}")
        # time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()

