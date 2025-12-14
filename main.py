import os
import json
import io
import requests
from google.cloud import pubsub_v1

def send_text_file_to_discord(webhook_url, text, filename="message.txt"):
    try:
        file_bytes = io.BytesIO(text.encode("utf-8"))
        file_bytes.seek(0)
        files = {"file": (filename, file_bytes, "text/plain")}
        resp = requests.post(webhook_url, files=files)
        resp.raise_for_status()
        print(f"Sent file to Discord: {resp.status_code}")
    except Exception as e:
        print(f"Error sending file to Discord: {e}")

def callback(message):
    print(f"Received message: {message.data}")
    try:
        raw = message.data
        try:
            data = json.loads(raw.decode("utf-8"))
            text = data.get("instruction") or data.get("text") or ""
        except Exception:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        
        if not text:
            print("No text found in message, acking and skipping.")
            message.ack()
            return

        webhook_url = os.environ.get("DISCORD_URL")
        if not webhook_url:
            print("Warning: DISCORD_URL not set")
        else:
            send_text_file_to_discord(webhook_url, text)

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