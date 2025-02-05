from flask import Flask, request, jsonify
import boto3
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"  # e.g., us-east-1

# Initialize S3 client
s3_client = boto3.client("s3")


# use post method
@app.route("/", methods=["POST"])
def upload_file():
    if "inputFile" not in request.files:
        return {"error": "No file part"}, 400

    file = request.files["inputFile"]
    if file.filename == "":
        return {"error": "No selected file"}, 400

    try:
        # Secure filename
        filename = secure_filename(file.filename)

        # Upload to S3
        s3_client.upload_fileobj(file, S3_BUCKET, filename)

        return jsonify({"message": "File uploaded successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
