from datetime import datetime
from azure.storage.blob import ContainerClient
import os

def get_blob_folder():
    today = datetime.now()
    container = ContainerClient.from_container_url(os.environ.get("UPLOADS_URL"))
    blobs = container.list_blobs()
    for blob in blobs:
        if today.strftime("%Y%m%d") in blob.name:
            folder = blob.name.split('/')[0]
            break
    try:
        return folder
    except NameError:
        return today.strftime("%Y%m%d_%H%M%S")