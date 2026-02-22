import os
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

ACCESS_TOKEN = os.environ["EVENTBRITE_TOKEN"]
PROJECT_ID   = os.environ["GCP_PROJECT"]
DATASET      = "raw_events"

client = bigquery.Client()

BASE_URL = "https://www.eventbriteapi.com/v3"
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

START_DATE = "2026-02-01T00:00:00Z"

def get_events():
    url = f"{BASE_URL}/users/me/events/"
    params = {"start_date.range_start": START_DATE}
    events = []

    while url:
        r = requests.get(url, headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()

        events.extend(data.get("events", []))
        url = data.get("pagination", {}).get("continuation")

    return events


def get_attendees(event_id):
    url = f"{BASE_URL}/events/{event_id}/attendees/"
    attendees = []

    while url:
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()

        attendees.extend(data.get("attendees", []))
        url = data.get("pagination", {}).get("continuation")

    return attendees


def load_to_bigquery(table, rows):
    if not rows:
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    job = client.load_table_from_json(rows, table_id)
    job.result()


def main(request):
    now = datetime.now(timezone.utc)
    events = get_events()

    event_rows = []
    attendee_rows = []

    for e in events:
        event_rows.append({
            "event_id": e["id"],
            "name": e["name"]["text"],
            "start": e["start"]["utc"],
            "end": e["end"]["utc"],
            "status": e["status"],
            "created": e["created"]
        })

        start = datetime.fromisoformat(e["start"]["utc"].replace("Z","+00:00"))
        end = datetime.fromisoformat(e["end"]["utc"].replace("Z","+00:00"))

        attendees = get_attendees(e["id"])

        for a in attendees:
            attendee_rows.append({
                "event_id": e["id"],
                "attendee_id": a["id"],
                "name": a["profile"].get("name"),
                "email": a["profile"].get("email"),
                "status": a["status"],
                "checked_in": a["checked_in"],
                "created": a["created"],
                "mode": "registration" if now < start else "attendance"
            })

    load_to_bigquery("events", event_rows)
    load_to_bigquery("attendees", attendee_rows)

    return "Success"
