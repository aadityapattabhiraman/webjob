import os
import time
import io
import json
import asyncio
from single_character import single_character_azure
from multi_character import multi_character_azure
from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from azure.servicebus import ServiceBusReceiveMode
from datetime import datetime
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from azure.cosmos import CosmosClient
from langchain.schema import SystemMessage, HumanMessage
from langchain_openai import AzureChatOpenAI
import base64
from azure.core import MatchConditions
from stitch_image_outside import stitch
from PIL import Image
from functools import reduce


CONNECTION_STR = os.environ["SERVICE_BUS"]
QUEUE_NAME = "queue"  # fallback default
CONCURRENCY = int(os.getenv("WORKERS", "1"))   # parallel workers
LOCK_RENEW_SECS = int(os.getenv("LOCK_RENEW_SECS", "300"))
preview_ids = []


def images_to_pdf(list):

    images = [Image.open(b).convert("RGB") for b in list]

    pdf_buffer = io.BytesIO()
    images[0].save(
        pdf_buffer, format="PDF", save_all=True, append_images=images[1:]
    )

    return pdf_buffer


def replace_text(list, characters):

    list = [
        reduce(lambda s, c: s.replace(c["key"], c["label"]), characters, _)
        for _ in list
    ]

    return list


async def get_prompt():

    prompt = """
You are writing one concise identity note for a character-rendering system that
matches reference portraits to characters in a scene.
This note is used ONLY to help match THIS portrait later. It must NOT cause
clothing, pose, body, or background changes.

Write ONE sentence (45–70 words) describing ONLY durable, head-related traits
visible in THIS portrait:

• Hair & hairline — length; curl/wave/coil pattern and relative coil size;
  density/volume; parting direction; hairline/forehead exposure;
  crown/temple details; color and subtle highlights/lowlights.
• Facial structure — overall face shape; jawline width/angle; cheek fullness;
  chin type; forehead height.
• Eyes & brows — eye size/shape/spacing; eyelid type; iris tone;
  eyelash density; brow thickness/arch.
• Durable marks & features — freckles/moles/scars/birthmarks;
  bindi/tilak (shape/placement/color); eyewear type; facial hair pattern
  (if any).
• Optional: broad skin-tone category + undertone (light/medium/dark;
  warm/cool/neutral).
• Include helpful negatives (e.g., “no glasses”, “no facial hair”).

Rules:
- Do NOT mention clothing, jewelry below the ears, posture, body build, age,
  gender, or facial expressions.
- Use proportional/artistic language, not biometric measurements.
- Begin with: “The [boy, girl, man, woman] in the reference image has …”
If the image doesn't have enough clear details for a complete description,
give a brief but useful response, stating that some features are less clear but
still focusing on any visible or discernible traits.
Return only the sentence.
    """

    return prompt


async def user_description(image):

    base64_image = base64.b64encode(image.read()).decode('utf-8')
    name = image.name

    mime_type = "image/png" if name.lower().endswith(".png") else "image/jpeg"

    # Create data URL for the image
    data_url = f"data:{mime_type};base64,{base64_image}"
    api_base = os.environ["gpt_4o_mini_openai_api_endpoint"]
    api_key = os.environ["gpt_4o_mini_openai_api_key"]
    deployment_name = os.environ["gpt_4o_mini_model_deployment_name"]
    api_version = "2025-04-01-preview"

    client = AzureChatOpenAI(
        deployment_name=deployment_name,
        model_name=deployment_name,
        openai_api_key=api_key,
        openai_api_version=api_version,
        azure_endpoint=api_base,
        timeout=30,
        max_retries=0,
    )

    sys = SystemMessage(content=await get_prompt())
    des = "Generate a detailed description of the image"
    msg = HumanMessage(
        content=[
            {"type": "text", "text": des},
            {"type": "image_url", "image_url": {"url": data_url}}
        ]
    )

    try:

        response = await client.ainvoke([sys, msg])
        return response.content

    except Exception as e:

        log_function(e)
        return "Unable to generate"


def log_function(log_content, clear_file=0):

    # 💡 Replace or load this from .env
    log_content = "[TESTING PREVIEW] " + str(log_content) + "\n"
    connection_string = os.getenv("connection_string")
    log_container_name = "logs"
    log_blob_name = "log_web_jobs.txt"

    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )
    blob_client = blob_service_client.get_blob_client(
        log_container_name,
        log_blob_name
    )

    # Clear the file if clear_file is set to 1
    if clear_file == 1:
        blob_client.upload_blob(
            b"",
            overwrite=True,
            content_settings=ContentSettings(content_type="text/plain")
        )

    # Download the existing log content if any
    try:

        existing = blob_client.download_blob().readall()

    except ResourceNotFoundError:

        existing = b""

    # Get IST time
    current_time_ist = datetime.now().strftime(
        "Current Time : %Y-%m-%d %H:%M:%S"
    )

    # Prepare the log content with timestamp
    timestamp_content = f"{current_time_ist} : "
    time_stamp_content_bytes = timestamp_content.encode("utf-8")
    log_content_bytes = log_content.encode("utf-8")

    # Combine the existing log with the new log entry
    blob_client.upload_blob(
        existing + time_stamp_content_bytes + log_content_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain")
    )


async def book_data_cosmos(book_id, books_container):

    log_function("Inside book")
    data = books_container.read_item(book_id, book_id)
    log_function("got book")
    return data


async def preview_dat(preview_id, container):

    query = f"SELECT * FROM c WHERE c.id='{preview_id}'"

    data = list(
        container.query_items(
            query=query,
            enable_cross_partition_query=True,
        )
    )[0]

    return data


async def modify_start_time(preview_id, container, endpoint_1, endpoint_2):

    query = f"SELECT * FROM c WHERE c.id='{preview_id}'"

    data = list(
        container.query_items(
            query=query,
            enable_cross_partition_query=True,
        )
    )[0]
    data["start_time"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    data["status"] = "in progress"
    data["deployment_1"] = endpoint_1
    data["deployment_2"] = endpoint_2
    container.replace_item(
        item=preview_id,
        body=data,
        etag=data["_etag"],
        match_condition=MatchConditions.IfNotModified,
    )

    return data


async def modify_end_time(preview_id, container):

    query = f"SELECT * FROM c WHERE c.id='{preview_id}'"

    data = list(
        container.query_items(
            query=query,
            enable_cross_partition_query=True,
        )
    )[0]
    data["end_time"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    data["status"] = "completed"
    container.replace_item(
        item=preview_id,
        body=data,
        etag=data["_etag"],
        match_condition=MatchConditions.IfNotModified,
    )

    return data


async def preview_data_cosmos(preview_id, container, user_desc):

    query = f"SELECT * FROM c WHERE c.id='{preview_id}'"

    data = list(
        container.query_items(
            query=query,
            enable_cross_partition_query=True,
        )
    )[0]
    data["user_description"] = user_desc
    container.replace_item(
        item=preview_id,
        body=data,
        etag=data["_etag"],
        match_condition=MatchConditions.IfNotModified,
    )
    return data


async def simulate_image_generation(data):

    await asyncio.sleep(90)
    return None


async def process_message(json_data, worker_name: str):

    preview_id = json_data["data"]
    client = CosmosClient(
        os.environ["cosmos_db_url"],
        os.environ["cosmos_db_key"]
    )
    container = (
        client.get_database_client("storybook_db")
        .get_container_client("previews_container")
    )
    books_container = (
        client.get_database_client("storybook_db")
        .get_container_client("books_container")
    )
    log_function(f"Processing: {preview_id}")
    deployment_1 = json_data.get("deployment_1").get("deployment-name")
    deployment_2 = json_data.get("deployment_1").get("deployment-name")
    await modify_start_time(preview_id, container, deployment_1, deployment_2)

    start = time.time()

    try:
        log_function(f"Started job: {worker_name}")
        blob_client = BlobServiceClient.from_connection_string(
            os.environ["connection_string"]
        )
        log_function("Getting preview data")
        book_data = await preview_dat(preview_id, container)
        book_id = book_data["bookId"]
        gender = book_data["variant"]
        user_data = book_data["characters"]
        user_id = book_data["ownerId"]
        log_function(book_data)
        log_function("Getting book data")
        book_data = await book_data_cosmos(book_id, books_container)

        log_function("Getting user images")
        user_images_task = asyncio.create_task(
            get_user_image_desc(user_data, blob_client)
        )
        user_images = None
        descriptions = {}

        image_tasks = []
        text = []
        count = 0
        stit = []

        for _ in book_data["metaData"]["preview_pages"]:

            path = book_data["pages"][_ - 1]["images"][gender]
            log_function("Getting template image")
            template_image = await get_image(path, blob_client)

            if not user_images:
                user_images, descriptions = await user_images_task

            log_function("Received user image and description")
            user_img = [
                user_images[key]
                for key in book_data["pages"][_ - 1]["character_data"]
                [gender].keys()
            ]
            stit.append(template_image)
            images = [template_image] + user_img

            user_desc = [
                f"* {key}: " + descriptions[key]
                for key in book_data["pages"][_ - 1]["character_data"]
                [gender].keys()
            ]

            await preview_data_cosmos(preview_id, container, str(user_desc))
            new_text = replace_text([book_data["pages"][_ - 1]["text"]], user_data)
            text.append(book_data["pages"][_ - 1]["text"])
            desc = [
                book_data["pages"][_ - 1]["vision_description"].get(gender)
            ]
            description = desc + user_desc

            if len(images) == 2:

                data = {
                    "page_num": _,
                    "preview_id": preview_id,
                    "book_id": book_id,
                    "user_id": user_id,
                    "images": images,
                    "description": description,
                    "text": new_text,
                    "gender": gender,
                    # "deployment": clients[count],
                }

                log_function("Sending to swap")
                image_tasks.append(single_character_azure(data))

            else:

                data = {
                    "page_num": _,
                    "preview_id": preview_id,
                    "book_id": book_id,
                    "user_id": user_id,
                    "images": images,
                    "description": description,
                    "text": new_text,
                    "gender": gender,
                    # "deployment": clients[count],
                }
                log_function("Sending to swap")
                image_tasks.append(multi_character_azure(data))
            count += 1

    except Exception as e:

        log_function(e)
        log_function("Nope")
        raise

    images = await asyncio.gather(*image_tasks)

    log_function("Received images")

    if len(images) == 0:
        return None

    log_function(f"Completed job: {worker_name}")
    end = time.time()
    await modify_end_time(preview_id, container)
    global preview_ids
    preview_ids.remove(preview_id)
    log_function(f"Time Taken: {end - start}")


async def get_user_image_desc(user_data, blob_client):

    tmp = "https://storyverseblobstorage.blob.core.windows.net/user-photos/"
    urls = [_["photoUrl"][len(tmp):] for _ in user_data]
    log_function(str(urls))

    image_task = [get_image(_, blob_client, "user-photos") for _ in urls]
    images = await asyncio.gather(*image_task)
    description_task = [user_description(_) for _ in images]
    descriptions = await asyncio.gather(*description_task)

    images = {
        _["key"]: images[i] for i, _ in enumerate(user_data)
    }
    descriptions = {
        _["key"]: descriptions[i]
        for i, _ in enumerate(user_data)
    }

    return images, descriptions


async def get_image(blob_path, blob_client, container_name="books"):

    blob_service_client = blob_client.get_blob_client(
        container_name, blob_path
    )

    blob = blob_service_client.download_blob()
    image = blob.readall()
    image = io.BytesIO(image)

    image.name = (
        "template_image.png"
        if blob_path.endswith("png")
        else "template_image.jpg"
    )

    return image


async def intermediate(msg, worker_name):

    global preview_ids
    json_data = json.loads(str(msg.message))
    preview_id = json_data["data"]

    if preview_id not in preview_ids:
        preview_ids.append(preview_id)
        asyncio.create_task(process_message(json_data, worker_name))


async def worker(worker_name: str):

    try:

        client = ServiceBusClient.from_connection_string(CONNECTION_STR)
        async with client:
            receiver = client.get_queue_receiver(
                queue_name=QUEUE_NAME,
                receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
                prefetch_count=20
            )
            async with receiver, AutoLockRenewer() as renewer:
                while True:
                    msgs = await receiver.receive_messages(
                        max_message_count=1, max_wait_time=5
                    )
                    if not msgs:
                        continue
                    msg = msgs[0]
                    renewer.register(
                        receiver, msg, max_lock_renewal_duration=LOCK_RENEW_SECS
                    )

                    try:

                        await intermediate(msg, worker_name)
                        await receiver.complete_message(msg)

                    except Exception as exc:

                        log_function(exc)
                        await receiver.abandon_message(msg)

    except Exception as e:
        log_function(e)


if __name__ == "__main__":

    asyncio.run(worker("w1"))
