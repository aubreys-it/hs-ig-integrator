import azure.functions as func
import logging
import os
import tempfile
import pysftp
from datetime import datetime
import json
from azure.storage.blob import ContainerClient
from ..modules import hsglob as g

# This function uploads a zip file to the HS FTP server from Azure Blob Storage

def main(req: func.HttpRequest) -> func.HttpResponse:

    ftp_host = g.ftp_host
    ftp_user = g.ftp_user
    ftp_pass = g.ftp_pass
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    container_client = ContainerClient.from_container_url(g.uploads_url)
    blobs=container_client.list_blobs()
    
    for blob in blobs:
        if datetime.today().strftime('%Y%m%d') in blob.name and blob.name.endswith('.zip'):
            logging.info(f'Processing blob: {blob.name}')
            blob_name = blob.name[blob.name.rfind('/') + 1:]  # Extract the blob name without the path
            logging.info(f'Blob name extracted: {blob_name}')
            blob_client = container_client.get_blob_client(blob.name)

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(blob_client.download_blob().readall())
                temp_file_path = temp_file.name
                logging.info(f'Temporary file created at: {temp_file_path}')

            # Connect to the FTP server
            with pysftp.Connection(host=ftp_host, username=ftp_user, password=ftp_pass, cnopts=cnopts) as sftp:
                # Upload the blob data to the FTP server
                sftp.put(temp_file_path, f"/datastore/Import/{blob_name}")
            
            # Clean up the temporary file
            os.remove(temp_file_path)

            break  # Exit after processing the first matching blob

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