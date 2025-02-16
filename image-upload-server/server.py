from flask import Flask, request, jsonify, Response
import boto3
from werkzeug.utils import secure_filename
import os, io
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

executor = ThreadPoolExecutor(max_workers=8)

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"  # e.g., us-east-1

# Initialize S3 client
s3_client = boto3.client("s3")

def get_attribute_value(item_name: str, attribute_name: str):
    """
    Fetches a specific attribute's value for a given item in SimpleDB.
    """
    # AWS SimpleDB Client
    client = boto3.client('sdb', region_name='us-east-1')

    # Define the domain
    domain_name = "1229602090-simpleDB"

    try:
        # Fetch attributes of the given item
        response = client.get_attributes(DomainName=domain_name, ItemName=item_name)

        # Check if attributes exist
        if "Attributes" in response:
            for attr in response["Attributes"]:
                if attr["Name"] == attribute_name:
                    return attr["Value"]  # Return the specific attribute value
            
            print(f"Attribute '{attribute_name}' not found in item '{item_name}'.")
            return None
        else:
            print(f"Item '{item_name}' not found.")
            return None
    except Exception as e:
        print(f"Error fetching attribute '{attribute_name}' for item '{item_name}': {e}")
        return None


def upload_to_s3(file_buffer, bucket, key):
    # This function runs in a separate thread.
    # Ensure file_buffer's pointer is at the beginning.
    file_buffer.seek(0)
    s3_client.upload_fileobj(file_buffer, bucket, key)

# use post method
@app.route("/", methods=["POST"])
def upload_file():
    if "inputFile" not in request.files:
        return Response("ERROR:No file part in http payload", mimetype="text/plain"), 400

    file = request.files["inputFile"]
    if file.filename == "":
        return Response("ERROR:No selected file", mimetype="text/plain"), 400

    try:
        # Secure filename
        filename = secure_filename(file.filename)

        name_without_extension = os.path.splitext(filename)[0]
        value = get_attribute_value(name_without_extension, "Results")

        # Read the file into memory to ensure the stream remains accessible
        file_bytes = file.read()
        file_buffer = io.BytesIO(file_bytes)

        # Upload to S3
        # Submit the upload task to the thread pool
        executor.submit(upload_to_s3, file_buffer, S3_BUCKET, filename)

        # Return plain text response in the required format
        return Response(f"{name_without_extension}:{value}", mimetype="text/plain"), 200

    except Exception as e:
        return Response(f"ERROR:{str(e)}", mimetype="text/plain"), 500

if __name__ == "__main__":
    app.run(threaded=True, port=8000)
