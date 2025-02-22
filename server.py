from flask import Flask, request
import boto3
import threading
import requests
# Constants
AWS_REGION = "us-east-1"

S3_BUCKET = "1229602090-in-bucket"
SDB_DOMAIN_NAME = "1229602090-simpleDB"

# Create synchronous boto3 clients
session = boto3.session.Session()
s3_client = session.client("s3", region_name=AWS_REGION)
sdb_client = session.client("sdb", region_name=AWS_REGION)

app = Flask(__name__)

def generate_presigned_url(filename):
    """Generate a pre-signed URL for direct S3 upload"""
    return s3_client.generate_presigned_url(
        'put_object',
        Params={'Bucket': S3_BUCKET, 'Key': filename},
        ExpiresIn=3600,  # URL valid for 1 hour
        HttpMethod="PUT"
    )

def uploadFiletoS3(file_data, filename):
    """Uploads file to S3 using a pre-signed URL from the server itself"""
    presigned_url = generate_presigned_url(filename)
    
    response = requests.put(presigned_url, data=file_data)
    if response.status_code == 200:
        return True
    else:
        print(f"Failed to upload: {response.text}")
        return False

# def uploadFiletoS3(file_data, filename):
#     s3_client.put_object(Body=file_data, Bucket=S3_BUCKET, Key=filename)

def identify_person(file_name):
    try:
        response = sdb_client.get_attributes(DomainName=SDB_DOMAIN_NAME, ItemName=file_name)
        if "Attributes" in response:
            for attr in response["Attributes"]:
                if attr["Name"] == "Results":
                    return attr["Value"]
        return None
    except Exception as e:
        print(f"Error fetching attribute: {e}")
        return None

@app.route("/", methods=["POST"])
def entrypoint():
    if 'inputFile' not in request.files:
        return "inputFile not present in files"

    inputFile = request.files['inputFile']
    filename = inputFile.filename
    
    uploadFiletoS3(inputFile.stream, filename)
    name_without_extension, _ = filename.rsplit(".", 1)

    name = identify_person(name_without_extension)

    s = name_without_extension + ":" + name

    return s

if(__name__ == "__main__"):
    app.run(host="0.0.0.0", port=8000)