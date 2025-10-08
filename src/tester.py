import os
import asyncio
import json
import time
import uuid
from pathlib import Path

import httpx

# Azure Service Bus (still commented out)
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

CONNECTION_STR = os.environ["CONNECTION_STR"]
QUEUE_NAME = "handle_rate"


async def test_preview():
    start = time.time()

    BASE_URL = "https://storyversewebapp-dkceefdmfqcwesc7.centralus-01.azurewebsites.net"
    BOOK_ID = "bk_maya-and-the-little-flower_e0de"
    OWNER_ID = str(uuid.uuid4())
    VARIANT = "boy"
    CHARACTERS = ["Maya", "Mrs.Thompson"]
    PERSONNAMES = ["Maya", "Mrs.Thompson"]
    IMAGE_FILES = ["../data/image1.jpg", "../data/image2.jpg"]
    PAGES_TO_RENDER = 2

    # Validate inputs
    assert len(CHARACTERS) == len(PERSONNAMES) == len(IMAGE_FILES), "Length mismatch between characters, names, and files."
    for f in IMAGE_FILES:
        if not Path(f).exists():
            raise FileNotFoundError(f"File {f} not found.")

    upload_url = f"{BASE_URL}/api/books/{BOOK_ID}/testing-upload"

    # Prepare form data
    form_data = {
        "variant": VARIANT,
        "characters": CHARACTERS,
        "personnames": PERSONNAMES,
    }

    # Prepare files
    files = []
    for file_path in IMAGE_FILES:
        with open(file_path, "rb") as f:
            content = f.read()
            files.append((
                "files",
                (Path(file_path).name, content, "image/jpeg")
            ))

    try:
        print(f"[{OWNER_ID}] Uploading images...")
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                upload_url,
                data=form_data,
                files=files,
                headers={"owner": OWNER_ID},
            )
            status = response.status_code
            text = response.text
    except Exception as e:
        print(f"[{OWNER_ID}] Upload failed with exception:", e)
        return

    print(f"[{OWNER_ID}] Upload response status:", status)

    try:
        upload_data = json.loads(text)
        print(f"[{OWNER_ID}] Upload response JSON:", upload_data)
    except Exception as e:
        print(f"[{OWNER_ID}] Upload failed. Raw response: {e}")
        print(text)
        return

    if not upload_data.get("success"):
        print(f"[{OWNER_ID}] Image validation or upload failed:", upload_data.get("message"))
        return

    preview_id = upload_data.get("previewId")

    # Optional: Send preview_id to Azure Service Bus
    async with ServiceBusClient.from_connection_string(CONNECTION_STR) as client:
        sender = client.get_queue_sender(queue_name=QUEUE_NAME)
        async with sender:
            message = ServiceBusMessage(json.dumps({"data": preview_id}))
            await sender.send_messages(message)

    end = time.time()
    print(f"[{OWNER_ID}] Time Taken: {end - start:.2f}s")


async def main():
    concurrency = 5
    tasks = []

    for _ in range(concurrency):
        tasks.append(asyncio.create_task(test_preview()))
        await asyncio.sleep(0.05)  # Optional delay

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
