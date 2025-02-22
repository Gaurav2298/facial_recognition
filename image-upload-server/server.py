import boto3
from fastapi import FastAPI, File, UploadFile, Response
import threading

app = FastAPI()

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"

# SimpleDB Config
SDB_DOMAIN_NAME = "1229602090-simpleDB"

# Create synchronous boto3 clients
s3_client = boto3.client("s3", region_name=S3_REGION)
sdb_client = boto3.client("sdb", region_name=S3_REGION)

def get_attribute_value(item_name: str, attribute_name: str):
    """Fetch attribute value from AWS SimpleDB synchronously"""
    try:
        response = sdb_client.get_attributes(DomainName=SDB_DOMAIN_NAME, ItemName=item_name, AttributeNames=[attribute_name])
        
        if "Attributes" in response:
            for attr in response["Attributes"]:
                if attr["Name"] == attribute_name:
                    return attr["Value"]
        return None
    except Exception as e:
        print(f"Error fetching attribute using get_attributes: {e}")
        return None

def uploadFileObj(file_bytes, bucket_name, s3_file_key):
    """Upload file to S3 synchronously"""
    try:
        s3_client.put_object(Body=file_bytes, Bucket=bucket_name, Key=s3_file_key)
        # print(f"Uploaded {s3_file_key} successfully")
    except Exception as e:
        print(f"Upload failed for {s3_file_key}: {e}")

@app.post("/")
def upload_file(inputFile: UploadFile = File(...)):
    try:
        filename = inputFile.filename
        name_without_extension, _ = filename.rsplit(".", 1)

        # Read file into bytes (keeping it in memory)
        file_bytes = inputFile.file.read()

        # Upload to S3 synchronously
        # uploadFileObj(file_bytes, S3_BUCKET, filename)
        s3_upload_thread = threading.Thread(
            target=uploadFileObj,
            args=(file_bytes, S3_BUCKET, filename)
        )
        s3_upload_thread.start()

        # Fetch attribute from SimpleDB synchronously
        sdb_result = get_attribute_value(name_without_extension, "Results")

        return Response(f"{name_without_extension}:{sdb_result}", media_type="text/plain")

    except Exception as e:
        return Response(f"ERROR: {str(e)}", media_type="text/plain", status_code=500)