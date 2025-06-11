import azure.functions as func
import logging, os, tempfile
import pysftp
from datetime import datetime
from azure.storage.blob import ContainerClient
import json

# This function uploads a zip file to the HS FTP server from Azure Blob Storage

def main(req: func.HttpRequest) -> func.HttpResponse:

    ftp_host = os.environ.get('FTP_HOST')
    ftp_user = os.environ.get('FTP_USER')
    ftp_pass = os.environ.get('FTP_PASS')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    container_client = ContainerClient.from_container_url(os.environ.get('UPLOADS_URL'))
    blobs=container_client.list_blobs()
    
    for blob in blobs:
        if datetime.today().strftime('%Y%m%d') in blob.name:
            blob_client = container_client.get_blob_client(blob.name)

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(blob_client.download_blob().readall())
                temp_file_path = temp_file.name
                logging.info(f'Temporary file created at: {temp_file_path}')

            # Connect to the FTP server
            with pysftp.Connection(host=ftp_host, username=ftp_user, password=ftp_pass, cnopts=cnopts) as sftp:
                # Upload the blob data to the FTP server
                sftp.put(temp_file_path, f"/datastore/Import/{blob.name}")
            logging.info(f'File {blob.name} uploaded to FTP server successfully.')

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