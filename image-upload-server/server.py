import boto3
from flask import Flask, request, Response
import threading
app = Flask(__name__)

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
    except Exception as e:
        print(f"Upload failed for {s3_file_key}: {e}")

@app.route("/", methods=["POST"])
def upload_file():
    try:
        if 'inputFile' not in request.files:
            return Response("ERROR: No file part", media_type="text/plain", status=400)
        
        inputFile = request.files['inputFile']
        filename = inputFile.filename
        name_without_extension, _ = filename.rsplit(".", 1)

        # Read file into bytes (keeping it in memory)
        file_bytes = inputFile.read()

        # Upload to S3 synchronously
        # uploadFileObj(file_bytes, S3_BUCKET, filename)
        s3_upload_thread = threading.Thread(
            target=uploadFileObj,
            args=(file_bytes, S3_BUCKET, filename)
        )
        s3_upload_thread.start()

        # Fetch attribute from SimpleDB synchronously
        sdb_result = get_attribute_value(name_without_extension, "Results")

        return Response(f"{name_without_extension}:{sdb_result}", mimetype="text/plain")

    except Exception as e:
        return Response(f"ERROR: {str(e)}", mimetype="text/plain", status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
