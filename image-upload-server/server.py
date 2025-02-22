import threading
from io import BytesIO
import boto3
from flask import Flask, request

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"

# Initialize S3 client
s3_client = boto3.client("s3")

sdb_client = boto3.client('sdb', region_name='us-east-1')
domain_name = "1229602090-simpleDB"

def uploadFileObj(file_data, bucket_name, s3_file_key):
    res = s3_client.put_object(Body=file_data, 
                        Bucket=bucket_name, 
                        Key=s3_file_key)
    print(f"res: ${res}\nSuccessfully uploaded the file to S3")
    
def get_image_class(image_file_name: str):
    res = sdb_client.get_attributes(DomainName=domain_name,
                            ItemName=image_file_name)
    
    return res

app = Flask(__name__)

@app.route("/", methods=['POST'])
def home():
    if request.method == 'POST':
        inputFile = request.files['inputFile']  # Access the file using the field name
        s3_file_key = inputFile.filename  # Use the filename as the S3 key
        
        file_data = inputFile.read()
        s3_upload_thread = threading.Thread(
            target=uploadFileObj,
            args=(file_data, S3_BUCKET, s3_file_key)
        )
        s3_upload_thread.start()
        
        res = get_image_class(s3_file_key.split('.')[0])
        
        s = f"{s3_file_key}:{res['Attributes'][0]['Value']}"

        return s
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
