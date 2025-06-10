import azure.functions as func
import logging, os, json
import datetime
from azure.storage.blob import ContainerClient

uploads_url = os.environ.get("UPLOADS_URL")
company_id = 964328777
concepts = {
    5: 1590
}

def create_employee_file(json_data):
    data = json.loads(json_data)
    out_file=[]

    for row in data:
        CompanyNumber = company_id
        ConceptNumber = concepts[row['StoreNum']]
        StoreNum = row['StoreNum']
        EmpID = row['EmpID']
        FirstName = row['FirstName']
        LastName = row['LastName']
        PhoneNo = row['PhoneNo']
        SmsNo = row['SmsNo']
        Address1 = row['Address1']
        City = row['City']
        Province_State = row['Province_State']
        PostalCode = row['PostalCode']
        FireDate = row['FireDate']
        Nickname = row['Nickname']
        HireDate = row['HireDate']
        BirthDate = row['BirthDate']
        EmpStatus = row['EmpStatus']

        out_row = f"{CompanyNumber}|{ConceptNumber}|{StoreNum}|{EmpID}|{FirstName}|" \
            f"{LastName}|{PhoneNo}|{SmsNo}||{Address1}|{City}|{Province_State}|" \
            f"{PostalCode}|{FireDate}|{Nickname}|{HireDate}||{BirthDate}|{EmpStatus}\n"
        out_file.append(out_row)
    return ''.join(out_file)

def get_blob_folder():
    today = datetime.datetime.now()
    container = ContainerClient.from_container_url(uploads_url)
    blobs = container.list_blobs()
    for blob in blobs:
        if today.strftime("%Y%m%d") in blob.name:
            return blob.name.split('/')[0]
        else:
            return today.strftime("%Y%m%d_%H%M%S")

def upload_employee_file(out_file):
    container = ContainerClient.from_container_url(uploads_url)
    blob_name = f"{get_blob_folder()}/empl_master_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    blob_client = container.get_blob_client(blob_name)
    blob_client.upload_blob(out_file, overwrite=True)
    return f"{uploads_url}/{blob_name}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()[1:-1]  # Remove the first and last characters (assumed to be brackets)
        logging.info(f"Request body: {req_body}")
    except ValueError:
        pass
        
    if req_body:
        out_file = create_employee_file(req_body)
        return json.dumps(upload_employee_file(out_file), ensure_ascii=False, indent=4, separators=(',', ': '), sort_keys=True)
    else:
        return func.HttpResponse(
             "No data provided",
             status_code=200
        )