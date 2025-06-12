import azure.functions as func
import logging
import io
import zipfile
import json
from datetime import datetime
from azure.storage.blob import ContainerClient
from ..modules import hsglob as g

#Constants
uploads_url = g.uploads_url

# This Azure Function zips files from a specified Azure Blob Storage container
# that match today's date and uploads the zip file back to a '__zipped' folder in the same container.

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    container_client = ContainerClient.from_container_url(uploads_url)
    blobs=container_client.list_blobs()
    zip_buffer = io.BytesIO()
    zip_file_name = 'aubreysinc_' + datetime.today().strftime('%Y%m%d_%H%M%S') + '.zip'
    
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for blob in blobs:
            if datetime.today().strftime('%Y%m%d') in blob.name and blob.name.endswith('.txt'):
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()
                zip_file.writestr(blob.name[blob.name.rfind('/')+1:], blob_data)

    zip_buffer.seek(0)

    blob_client = container_client.get_blob_client('__zipped/' + zip_file_name)
    # Upload the zip file to the blob storage
    blob_client.upload_blob(zip_buffer.getvalue(), overwrite=True)
    # If req_body is not None, return it as a JSON response
    req_body = {
        'message': 'Zip file created successfully'
    }
    logging.info(f'Returning response: {req_body}')

    if req_body:
        return json.dumps(req_body)
    else:
        return func.HttpResponse(
             "No data provided",
             status_code=400
        )