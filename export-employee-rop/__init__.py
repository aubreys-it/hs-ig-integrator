import azure.functions as func
import logging
import json
from datetime import datetime
from azure.storage.blob import ContainerClient
from ..modules import hsfunc
from ..modules import hsglob as g

# Constants
company_id = g.company_id
concepts = g.concepts
uploads_url = g.uploads_url

def create_rop_file(json_data):
    data = json.loads(json_data)
    out_file=[]
    for row in data:
        CompanyNumber = company_id
        ConceptNumber = concepts.get(row['StoreNum'], 99)  # Default to 99 if not found
        StoreNum = row['StoreNum']
        EmpID = row['EmpID']
        JobNo = row['JobNo']
        PayRate = row['PayRate']
        OvertimeFactor = row['OverTimeFactor']
        PrimaryFlag = row['PrimaryFlag']

        if ConceptNumber !=99:
            out_row = f"{CompanyNumber}|{ConceptNumber}|{StoreNum}|{EmpID}|" \
                f"{JobNo}|{PayRate}|{OvertimeFactor}||{PrimaryFlag}\n"
            out_file.append(out_row)
        
    return ''.join(out_file)

def upload_rop_file(out_file):
    container = ContainerClient.from_container_url(uploads_url)
    blob_name = f"{hsfunc.get_blob_folder()}/empl_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    blob_client = container.get_blob_client(blob_name)
    blob_client.upload_blob(out_file, overwrite=True)
    return {"blob_file": f"{uploads_url}/{blob_name}"}

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = str(req.get_json()).replace("'", '"')
    except ValueError:
        pass
        
    if req_body:
        out_file = create_rop_file(req_body)
        return json.dumps(upload_rop_file(out_file), ensure_ascii=False, indent=4, separators=(',', ': '), sort_keys=True)
    else:
        return func.HttpResponse(
             "No data provided",
             status_code=200
        )