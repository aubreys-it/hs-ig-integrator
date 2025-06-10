import azure.functions as func
import logging, zipfile
from azure.storage.blob import ContainerClient
from ..modules import core
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = str(req.get_json()).replace("'", '"')
    except ValueError:
        pass

    req_body = {"folder": core.get_blob_folder()}    
    logging.info(f"Request body: {req_body}")

    if req_body:
        return json.dumps(req_body)
    else:
        return func.HttpResponse(
             "No data provided",
             status_code=200
        )