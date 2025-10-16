import os
import pytz
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError



def log_function(log_content,clear_file=0):
  
        # 💡 Replace or load this from .env
        connection_string = os.getenv("connection_string")
        log_container_name = "logs"
        log_blob_name = "log_web_app_2.txt"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(log_container_name, log_blob_name)


        if clear_file==1:
         blob_client.upload_blob(b"", overwrite=True, content_settings=ContentSettings(content_type="text/plain"))
    
        log_content=str(log_content)+"\n"
        # # download existing
        # stream = blob_client.download_blob()
        # existing = stream.readall()              # bytes
        try:
            existing = blob_client.download_blob().readall()
        except ResourceNotFoundError:
            existing = b""
        
        # Get IST time
        ist = pytz.timezone("Asia/Kolkata")
        current_time_ist = datetime.now(ist).strftime("Current Time : %Y-%m-%d %H:%M:%S")
        
        # File content
        timestamp_content = f"{current_time_ist} : "
        time_stamp_content_bytes = timestamp_content.encode("utf-8")
        
        # log_content = f"This is a log statement\n"
        log_content_bytes=log_content.encode("utf-8")
        
        # combine and reupload
        blob_client.upload_blob(
            existing + time_stamp_content_bytes + log_content_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="text/plain")
        )


def log_moderation(log_content):

    connection_string = os.getenv("connection_string")
    log_container_name = "logs"
    log_blob_name = "moderation.txt"
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )
    blob_client = blob_service_client.get_blob_client(
        log_container_name, log_blob_name
    )

    log_content = log_content + "\n"
    # download existing
    try:
        stream = blob_client.download_blob()
        existing = stream.readall()              # bytes
    except:
        existing = "".encode("utf-8")

    # Get IST time
    ist = pytz.timezone("Asia/Kolkata")
    current_time_ist = datetime.now(ist).strftime(
        "Current Time : %Y-%m-%d %H:%M:%S"
    )

    # File content
    timestamp_content = f"{current_time_ist} : "
    time_stamp_content_bytes = timestamp_content.encode("utf-8")

    # log_content = f"This is a log statement\n"
    log_content_bytes = log_content.encode("utf-8")

    # combine and reupload
    blob_client.upload_blob(
        existing + time_stamp_content_bytes + log_content_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain")
    )
