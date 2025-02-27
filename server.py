from flask import Flask, request
import boto3
import threading
# Constants
AWS_REGION = "us-east-1"

S3_BUCKET = "1229602090-in-bucket"
SDB_DOMAIN_NAME = "1229602090-simpleDB"

# allow reusing the session across requests 
# so we won't have to create client again and again
session = boto3.session.Session()
s3_client = session.client("s3", region_name=AWS_REGION)
sdb_client = session.client("sdb", region_name=AWS_REGION)

app = Flask(__name__)

def uploadFiletoS3(file_data, filename):
    s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=file_data)

def identify_person(file_name):
    response = sdb_client.get_attributes(DomainName=SDB_DOMAIN_NAME, ItemName=file_name)
    if "Attributes" in response:
        for attr in response["Attributes"]:
            if attr["Name"] == "Results": # this is the attr that hold name
                return attr["Value"]
    return None

@app.route("/", methods=["POST"])
def entrypoint():
    if 'inputFile' not in request.files:
        return "inputFile not present in http request.files"

    inputFile = request.files['inputFile']
    filename = inputFile.filename
    # load the file in local memory
    # this is needed because asyncio operation looses file context once added to thread
    file_data = inputFile.read() 
    s3_thread = threading.Thread(target=uploadFiletoS3, args=(file_data, filename))
    s3_thread.start()

    # find file name w/o extention    
    file_name_without_extension, _ = filename.rsplit(".", 1)
    persons_name = identify_person(file_name_without_extension)

    response = file_name_without_extension + ":" + persons_name

    return response

if(__name__ == "__main__"):
    app.run(host="0.0.0.0", port=8000)