import azure.functions as func
import logging, os
from ftplib import FTP
from datetime import datetime
from azure.storage.blob import ContainerClient
import json

# This Azure Function zips files from a specified Azure Blob Storage container
# that match today's date and uploads the zip file back to a '__zipped' folder in the same container.

def main(req: func.HttpRequest) -> func.HttpResponse:

    ftp_host = os.environ.get('FTP_HOST')
    ftp_user = os.environ.get('FTP_USER')
    ftp_pass = os.environ.get('FTP_PASS')
    
    container_client = ContainerClient.from_container_url(os.environ.get('UPLOADS_URL'))
    blobs=container_client.list_blobs()
    
    for blob in blobs:
        if datetime.today().strftime('%Y%m%d') in blob.name:
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob().readall()

            # Connect to the FTP server
            with FTP(ftp_host) as ftp:
                ftp.login(user=ftp_user, passwd=ftp_pass)
                # Upload the blob data to the FTP server
                ftp.storbinary(f'STOR datastore/Import{blob.name}', blob_data)

    req_body = {
        'message': 'Zip file uploaded successfully'
    }
    logging.info(f'Returning response: {req_body}')

    if req_body:
        return json.dumps(req_body)
    else:
        return func.HttpResponse(
             "No data provided",
             status_code=400
        )