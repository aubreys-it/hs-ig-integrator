import azure.functions as func
import logging, os
import zipfile
from datetime import datetime
from azure.storage.blob import ContainerClient
from ..modules import core
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    tempPath = os.environ.get('TEMP', '/tmp')
    zipfile_name = 'aubreysinc_' + datetime.today().strftime('%Y%m%d_%H%M%S') + '.zip'
    logging.info(f'Temporary path: {tempPath}')
    logging.info(f'Zip file name: {zipfile_name}')

    '''go through all the blob folders and look for one that starts with todays date. then zip the folder with all of its contents'''
    container_client = ContainerClient.from_container_url(os.environ.get('UPLOADS_URL'))
    blobs_list = container_client.list_blobs()
    req_body = None
    for blob in blobs_list:
        if blob.name.startswith(datetime.today().strftime('%Y%m%d')):
            logging.info(f'Found blob: {blob.name}')
            blob_client = container_client.get_blob_client(blob)
            with open(os.path.join(tempPath, zipfile_name), 'wb') as f:
                data = blob_client.download_blob()
                f.write(data.readall())
            logging.info(f'Blob {blob.name} downloaded to {tempPath}/{zipfile_name}')
    with zipfile.ZipFile(os.path.join(tempPath, zipfile_name), 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(os.path.join(tempPath, zipfile_name), arcname=blob.name)
        req_body= zipfile_name

    if not req_body:
        logging.info('No blobs found for today.')
        return func.HttpResponse(
            "No blobs found for today",
            status_code=404
        )
    # Return the zip file content as a response
    if req_body and isinstance(req_body, str):
        req_body = {'zip_file': req_body}
        blob_client = container_client.get_blob_client(zipfile_name)
        blob_client.upload_blob(req_body, overwrite=True)
    elif req_body and isinstance(req_body, list):
        req_body = {'zip_files': req_body}
    else:
        logging.error('Unexpected response format from zip_folder function.')
        return func.HttpResponse(
            "Error processing zip files",
            status_code=500
        )
    # If req_body is not None, return it as a JSON response
    logging.info(f'Returning response: {req_body}')

    

    if req_body:
        return json.dumps(req_body)
    else:
        return func.HttpResponse(
             "No data provided",
             status_code=200
        )