from flask import Flask, request, Response
import boto3
from werkzeug.utils import secure_filename
import os, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

app = Flask(__name__)

executor = ThreadPoolExecutor(max_workers=100)

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"

# Initialize S3 client
s3_client = boto3.client("s3")

# Global cache for pre-signed URLs: { key: (url, expiry_time) }
presigned_url_cache = {}
cache_lock = threading.Lock()

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

def generate_presigned_url(key, expiration=900):
    """
    Generate (or retrieve from cache) a pre-signed URL for an S3 PUT operation.
    The URL will expire in 'expiration' seconds (default 15 minutes).
    A buffer is subtracted to ensure the URL is valid when used.
    """
    global presigned_url_cache
    with cache_lock:
        # If the key is cached and the URL hasn’t expired yet, return it
        if key in presigned_url_cache:
            url, expiry = presigned_url_cache[key]
            if time.time() < expiry:
                return url

        try:
            url = s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': S3_BUCKET, 'Key': key},
                ExpiresIn=expiration
            )
            # Cache it with a slight buffer (e.g., 30 seconds) before actual expiration
            presigned_url_cache[key] = (url, time.time() + expiration - 30)
            return url
        except Exception as e:
            print(f"Error generating pre-signed URL: {e}")
            return None

def upload_via_presigned_url(url, file_stream):
    """
    Use the pre-signed URL to upload the file via an HTTP PUT request.
    """
    try:
        file_stream.seek(0)
        # Perform PUT request. S3 expects the file contents in the body.
        response = requests.put(url, data=file_stream)
        if response.status_code in (200, 204):
            return True
        else:
            print(f"Error uploading via pre-signed URL: {response.status_code} {response.text}")
            return False
    except Exception as e:
        print(f"Exception during upload via pre-signed URL: {e}")
        return False

# def upload_to_s3(file_buffer, bucket, key):
#     """Upload file buffer to S3"""
#     try:
#         file_buffer.seek(0)
#         s3_client.upload_fileobj(file_buffer, bucket, key)
#         return key  # Return filename on success
#     except Exception as e:
#         print(f"Upload failed for {key}: {e}")
#         return None  # Return None on failure

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
        # future_sdb = executor.submit(get_attribute_value, name_without_extension, "Results")
        # future_s3 = executor.submit(upload_to_s3, file_buffer, S3_BUCKET, filename)

        # # Wait for both tasks to complete
        # sdb_result = future_sdb.result()
        # s3_result = future_s3.result()

        # # Check if S3 upload failed
        # if s3_result is None:
        #     return Response(f"ERROR: Failed to upload {filename} to S3", mimetype="text/plain"), 500

        # Generate the pre-signed URL and fetch the SimpleDB attribute concurrently
        future_presigned = executor.submit(generate_presigned_url, filename)
        future_sdb = executor.submit(get_attribute_value, name_without_extension, "Results")

        presigned_url = future_presigned.result()
        sdb_result = future_sdb.result()

        if presigned_url is None:
            return Response(f"ERROR: Failed to generate pre-signed URL for {filename}", mimetype="text/plain"), 500

        # Immediately attempt to upload the file using the pre-signed URL
        upload_success = upload_via_presigned_url(presigned_url, file_buffer)
        if not upload_success:
            return Response(f"ERROR: Failed to upload {filename} to S3 via pre-signed URL", mimetype="text/plain"), 500

        return Response(f"{name_without_extension}:{sdb_result}", mimetype="text/plain"), 200

    except Exception as e:
        return Response(f"ERROR: {str(e)}", mimetype="text/plain"), 500

if __name__ == "__main__":
    app.run(threaded=True, port=8000)