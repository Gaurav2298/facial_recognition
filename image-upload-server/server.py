from flask import Flask, request, Response
import boto3
from werkzeug.utils import secure_filename
import os, io
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

executor = ThreadPoolExecutor(max_workers=100)

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"

# Initialize S3 client
s3_client = boto3.client("s3")

def get_attribute_value(item_name: str, attribute_name: str):
    client = boto3.client('sdb', region_name='us-east-1')
    domain_name = "1229602090-simpleDB"

    try:
        response = client.get_attributes(DomainName=domain_name, ItemName=item_name)
        if "Attributes" in response:
            for attr in response["Attributes"]:
                if attr["Name"] == attribute_name:
                    return attr["Value"]
        return None
    except Exception as e:
        print(f"Error fetching attribute: {e}")
        return None

def upload_to_s3(file_buffer, bucket, key):
    """Upload file buffer to S3"""
    try:
        file_buffer.seek(0)
        s3_client.upload_fileobj(file_buffer, bucket, key)
        return key  # Return filename on success
    except Exception as e:
        print(f"Upload failed for {key}: {e}")
        return None  # Return None on failure

@app.route("/", methods=["POST"])
def upload_file():
    if "inputFile" not in request.files:
        return Response("ERROR: No file part in HTTP payload", mimetype="text/plain"), 400

    file = request.files["inputFile"]
    if file.filename == "":
        return Response("ERROR: No selected file", mimetype="text/plain"), 400

    try:
        filename = secure_filename(file.filename)
        name_without_extension = os.path.splitext(filename)[0]
        # value = get_attribute_value(name_without_extension, "Results")

        # file_bytes = file.read()
        # file_buffer = io.BytesIO(file_bytes)
        file_buffer = file.stream  # Stream instead of reading into memory

        # Submit the upload task asynchronously
        # future = executor.submit(upload_to_s3, file_buffer, S3_BUCKET, filename)

        # Submit both tasks in parallel
        future_sdb = executor.submit(get_attribute_value, name_without_extension, "Results")
        future_s3 = executor.submit(upload_to_s3, file_buffer, S3_BUCKET, filename)

        # Wait for both tasks to complete
        sdb_result = future_sdb.result()
        s3_result = future_s3.result()

        # Check if S3 upload failed
        if s3_result is None:
            return Response(f"ERROR: Failed to upload {filename} to S3", mimetype="text/plain"), 500


        # Wait for completion using as_completed()
        # for completed_future in as_completed([future]):
        #     result = completed_future.result()
        #     if result is None:
        #         return Response(f"ERROR: Failed to upload {filename} to S3", mimetype="text/plain"), 500

        return Response(f"{name_without_extension}:{sdb_result}", mimetype="text/plain"), 200

    except Exception as e:
        return Response(f"ERROR: {str(e)}", mimetype="text/plain"), 500

if __name__ == "__main__":
    app.run(threaded=True, port=8000)
