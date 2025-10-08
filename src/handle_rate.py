import asyncio
import json
import os
from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from azure.servicebus import ServiceBusReceiveMode, ServiceBusMessage
from azure.cosmos import CosmosClient
from datetime import datetime
from typing import Dict
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError
from azure.core import MatchConditions


CONNECTION_STR = os.environ["SERVICE_BUS"]
QUEUE_NAME = "queue"  # fallback default
LOCK_RENEW_SECS = int(os.getenv("LOCK_RENEW_SECS", "300"))
COSMOS_URL = os.getenv("cosmos_db_url")
COSMOS_KEY = os.getenv("cosmos_db_key")
DATABASE_NAME = "storybook_db"
preview_ids = []

cosmos_client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)
database = cosmos_client.get_database_client(DATABASE_NAME)
previews_container = database.get_container_client("previews_container")
rate_limits = {
    "westus": {
        "gpt-image-1-1": [],
        "gpt-image-1-2": [],
        "gpt-image-1-3": [],
        "gpt-image-1-13": [],
        "gpt-image-1-14": [],
        "gpt-image-1-15": [],
    },
    "middle-east": {
        "gpt-image-1-4": [],
        "gpt-image-1-5": [],
        "gpt-image-1-6": [],
        "gpt-image-1-16": [],
        "gpt-image-1-17": [],
        "gpt-image-1-18": [],
    },
    "poland": {
        "gpt-image-1-7": [],
        "gpt-image-1-8": [],
        "gpt-image-1-22": [],
        "gpt-image-1-23": [],
        "gpt-image-1-24": [],
    },
    "eastus": {
        "gpt-image-1-10": [],
        "gpt-image-1-11": [],
        "gpt-image-1-12": [],
    },
    "sweden": {
        "gpt-image-1-19": [],
        "gpt-image-1-20": [],
        "gpt-image-1-21": [],
    },
}


def log_function(log_content, clear_file=0):

    log_content = "[HANDLE PREVIEW] " + str(log_content) + "\n"
    connection_string = os.getenv("connection_string")
    log_container_name = "logs"
    log_blob_name = "log_web_jobs.txt"

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )
    blob_client = blob_service_client.get_blob_client(
        log_container_name, log_blob_name
    )

    if clear_file == 1:
        blob_client.upload_blob(
            b"",
            overwrite=True,
            content_settings=ContentSettings(content_type="text/plain")
        )

    try:
        existing = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        existing = b""

    current_time_ist = datetime.now().strftime(
        "Current Time : %Y-%m-%d %H:%M:%S"
    )
    timestamp_content = f"{current_time_ist} : "
    time_stamp_content_bytes = timestamp_content.encode("utf-8")
    log_content_bytes = log_content.encode("utf-8")

    blob_client.upload_blob(
        existing + time_stamp_content_bytes + log_content_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain")
    )


def get_deployments():

    deployments = {
        "westus": [
            {
                "deployment-name": os.environ["deployment_1"],
                "api_key": os.environ["api_key_1"],
                "endpoint": os.environ["endpoint_1"],
            },
            {
                "deployment-name": os.environ["deployment_2"],
                "api_key": os.environ["api_key_1"],
                "endpoint": os.environ["endpoint_1"],
            },
            {
                "deployment-name": os.environ["deployment_3"],
                "api_key": os.environ["api_key_1"],
                "endpoint": os.environ["endpoint_1"],
            },
            {
                "deployment-name": os.environ["deployment_13"],
                "api_key": os.environ["api_key_1"],
                "endpoint": os.environ["endpoint_1"],
            },
            {
                "deployment-name": os.environ["deployment_14"],
                "api_key": os.environ["api_key_1"],
                "endpoint": os.environ["endpoint_1"],
            },
            {
                "deployment-name": os.environ["deployment_15"],
                "api_key": os.environ["api_key_1"],
                "endpoint": os.environ["endpoint_1"],
            },
        ],
        "middle-east": [
            {
                "deployment-name": os.environ["deployment_4"],
                "api_key": os.environ["api_key_2"],
                "endpoint": os.environ["endpoint_2"],
            },
            {
                "deployment-name": os.environ["deployment_5"],
                "api_key": os.environ["api_key_2"],
                "endpoint": os.environ["endpoint_2"],
            },
            {
                "deployment-name": os.environ["deployment_6"],
                "api_key": os.environ["api_key_2"],
                "endpoint": os.environ["endpoint_2"],
            },
            {
                "deployment-name": os.environ["deployment_16"],
                "api_key": os.environ["api_key_2"],
                "endpoint": os.environ["endpoint_2"],
            },
            {
                "deployment-name": os.environ["deployment_17"],
                "api_key": os.environ["api_key_2"],
                "endpoint": os.environ["endpoint_2"],
            },
            {
                "deployment-name": os.environ["deployment_18"],
                "api_key": os.environ["api_key_2"],
                "endpoint": os.environ["endpoint_2"],
            },
        ],
        "poland": [
            {
                "deployment-name": os.environ["deployment_7"],
                "api_key": os.environ["api_key_3"],
                "endpoint": os.environ["endpoint_3"],
            },
            {
                "deployment-name": os.environ["deployment_8"],
                "api_key": os.environ["api_key_3"],
                "endpoint": os.environ["endpoint_3"],
            },
            {
                "deployment-name": os.environ["deployment_22"],
                "api_key": os.environ["api_key_3"],
                "endpoint": os.environ["endpoint_3"],
            },
            {
                "deployment-name": os.environ["deployment_23"],
                "api_key": os.environ["api_key_3"],
                "endpoint": os.environ["endpoint_3"],
            },
            {
                "deployment-name": os.environ["deployment_24"],
                "api_key": os.environ["api_key_3"],
                "endpoint": os.environ["endpoint_3"],
            },
        ],
        "eastus": [
            {
                "deployment-name": os.environ["deployment_10"],
                "api_key": os.environ["api_key_4"],
                "endpoint": os.environ["endpoint_4"],
            },
            {
                "deployment-name": os.environ["deployment_11"],
                "api_key": os.environ["api_key_4"],
                "endpoint": os.environ["endpoint_4"],
            },
            {
                "deployment-name": os.environ["deployment_12"],
                "api_key": os.environ["api_key_4"],
                "endpoint": os.environ["endpoint_4"],
            },
        ],
        "sweden": [
            {
                "deployment-name": os.environ["deployment_19"],
                "api_key": os.environ["api_key_5"],
                "endpoint": os.environ["endpoint_5"],
            },
            {
                "deployment-name": os.environ["deployment_20"],
                "api_key": os.environ["api_key_5"],
                "endpoint": os.environ["endpoint_5"],
            },
            {
                "deployment-name": os.environ["deployment_21"],
                "api_key": os.environ["api_key_5"],
                "endpoint": os.environ["endpoint_5"],
            },
        ],
    }

    return deployments


def get_available_deployment(deployments: Dict) -> dict:

    global rate_limits
    current_time = datetime.now()

    for zone, zone_deployments in deployments.items():
        for deployment in zone_deployments:
            deployment_name = deployment["deployment-name"]

            # Get the list of timestamps for the current deployment
            timestamps = rate_limits.get(zone, {}).get(deployment_name, [])

            # Count the number of timestamps within the last 60 seconds
            recent_count = sum(
                1 for ts in timestamps
                if (current_time - datetime.fromisoformat(ts)).total_seconds() < 60
            )

            if recent_count < 3:
                return deployment

    return {}


async def update_timestamps(deploymenter_1, deploymenter_2):

    global rate_limits
    current_time = datetime.now()

    for zone in rate_limits.keys():

        for deployment in rate_limits[zone].keys():

            rate_limits[zone][deployment] = [
                ts
                for ts in rate_limits[zone][deployment]
                if (
                    current_time - datetime.fromisoformat(ts)
                ).total_seconds() < 60
            ]

            if deployment == deploymenter_1["deployment-name"]:
                rate_limits[zone][deployment].append(str(current_time))

            if deployment == deploymenter_2["deployment-name"]:
                rate_limits[zone][deployment].append(str(current_time))


async def modify_start_time(preview_id, container):

    query = f"SELECT * FROM c WHERE c.id='{preview_id}'"

    data = list(
        container.query_items(
            query=query,
            enable_cross_partition_query=True,
        )
    )[0]
    data["request_time"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    container.replace_item(
        item=preview_id,
        body=data,
        etag=data["_etag"],
        match_condition=MatchConditions.IfNotModified,
    )

    return data


async def process_message(preview_id):

    global rate_limits, preview_ids
    preview_id = json.loads(str(preview_id.message))["data"]

    if preview_id in preview_ids:
        return

    preview_ids.append(preview_id)

    if len(preview_ids) > 50:
        preview_ids = preview_ids[30:]
    log_function(f"Starting to Process: {preview_id}")

    while True:
        deployments = get_deployments()
        deployment_1 = get_available_deployment(deployments)
        deployment_2 = get_available_deployment(deployments)
        await update_timestamps(deployment_1, deployment_2)

        if deployment_1 != {} and deployment_2 != {}:
            break

    try:

        client = ServiceBusClient.from_connection_string(CONNECTION_STR)
        sender = client.get_queue_sender(queue_name=QUEUE_NAME)
        message = ServiceBusMessage(json.dumps(
            {"data": preview_id, "deployment_1": deployment_1, "deployment_2": deployment_2}
        ))

        await sender.send_messages(message)
        await sender.close()
        await client.close()

    except Exception as e:

        log_function(e)
        log_function("Nope")
        raise

    await modify_start_time(preview_id, previews_container)


async def worker():

    try:

        client = ServiceBusClient.from_connection_string(CONNECTION_STR)
        async with client:
            receiver = client.get_queue_receiver(
                queue_name="handle_rate",
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
                        receiver,
                        msg,
                        max_lock_renewal_duration=LOCK_RENEW_SECS
                    )
                    try:
                        await process_message(msg)
                    except Exception as exc:
                        log_function(exc)
                        await receiver.abandon_message(msg)
                    else:
                        await receiver.complete_message(msg)

    except Exception as e:

        log_function(e)


if __name__ == "__main__":

    asyncio.run(worker())
