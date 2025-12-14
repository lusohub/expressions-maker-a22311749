import os
import json
import io
import re
import datetime
import requests
from google.cloud import pubsub_v1

def send_text_file_to_discord(webhook_url, text, filename="client.txt"):
    try:
        file_bytes = io.BytesIO(text.encode("utf-8"))
        file_bytes.seek(0)
        files = {"file": (filename, file_bytes, "text/plain")}
        resp = requests.post(webhook_url, files=files, timeout=15)
        resp.raise_for_status()
        print(f"Sent file to Discord: {resp.status_code}")
    except Exception as e:
        print(f"Error sending file to Discord: {e}")

def _safe_filename(name):
    name = (name or "client").strip()
    name = re.sub(r"[^A-Za-z0-9\-_]", "_", name)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return f"client_{name}_{ts}.txt"

def _format_client_text(client):
    now = datetime.datetime.utcnow().isoformat() + "Z"
    fields = [
        ("Name", client.get("name")),
        ("Email", client.get("email")),
        ("Phone", client.get("phone")),
        ("Company", client.get("company")),
        ("Address", client.get("address")),
        ("Preferred contact", client.get("preferred_contact")),
        ("Notes", client.get("notes")),
    ]
    lines = [f"Created: {now}", ""]
    for label, value in fields:
        lines.append(f"{label}: {value or ''}")
    return "\n".join(lines)

def callback(message):
    print(f"Received message: {message.data}")
    try:
        raw = message.data
        client_data = {}
        # try JSON decode
        try:
            decoded = json.loads(raw.decode("utf-8"))
            # accept several possible shapes: { client: {...} }, { form: {...} }, or direct object
            if isinstance(decoded, dict):
                client_data = decoded.get("client") or decoded.get("form") or decoded
            else:
                client_data = {"notes": str(decoded)}
        except Exception:
            # fallback to plain text
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            client_data = {"notes": text}

        # ensure at least something present
        if not any(client_data.get(k) for k in ("name", "email", "phone", "notes")):
            print("No client fields found, acking and skipping.")
            message.ack()
            return

        webhook_url = os.environ.get("DISCORD_URL")
        if not webhook_url:
            print("Warning: DISCORD_URL not set")
        else:
            filename = _safe_filename(client_data.get("name"))
            text = _format_client_text(client_data)
            send_text_file_to_discord(webhook_url, text, filename=filename)

        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

def main():
    project_id = os.environ.get("GCP_PROJECT_ID")
    subscription_id = os.environ.get("PUBSUB_SUBSCRIPTION_ID")

    if not project_id or not subscription_id:
        print("Error: GCP_PROJECT_ID and PUBSUB_SUBSCRIPTION_ID must be set")
        return

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    print(f"Listening to: {subscription_path}")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages on Pub/Sub...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Stopped listening")

if __name__ == "__main__":
    main()