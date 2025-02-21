import asyncio
import boto3
from fastapi import FastAPI, File, UploadFile, Response

app = FastAPI()

# Configure S3
S3_BUCKET = "1229602090-in-bucket"
S3_REGION = "us-east-1"

# SimpleDB Config
SDB_DOMAIN_NAME = "1229602090-simpleDB"

# Create synchronous boto3 clients
s3_client = boto3.client("s3", region_name=S3_REGION)
sdb_client = boto3.client("sdb", region_name=S3_REGION)

async def get_attribute_value(item_name: str, attribute_name: str):
    """Fetch attribute value from AWS SimpleDB asynchronously"""
    try:
        query = f"SELECT {attribute_name} FROM `{SDB_DOMAIN_NAME}` WHERE itemName() = '{item_name}'"
        response = await asyncio.to_thread(
            sdb_client.select,
            SelectExpression=query
        )

        if "Items" in response:
            for item in response["Items"]:
                for attr in item["Attributes"]:
                    if attr["Name"] == attribute_name:
                        return attr["Value"]
        return None
    except Exception as e:
        print(f"Error fetching attribute using SELECT: {e}")
        return None

async def uploadFileObj(file_bytes, bucket_name, s3_file_key):
    """Upload file to S3 asynchronously"""
    try:
        await asyncio.to_thread(
            s3_client.put_object,
            Body=file_bytes,
            Bucket=bucket_name,
            Key=s3_file_key
        )
        print(f"Uploaded {s3_file_key} successfully")
    except Exception as e:
        print(f"Upload failed for {s3_file_key}: {e}")

@app.post("/")
async def upload_file(inputFile: UploadFile = File(...)):
    try:
        filename = inputFile.filename
        name_without_extension, _ = filename.rsplit(".", 1)

        # Fetch attribute from SimpleDB asynchronously
        sdb_result = await get_attribute_value(name_without_extension, "Results")

        # Read file into bytes (keeping it in memory)
        file_bytes = await inputFile.read()

        # Upload to S3 asynchronously without threading
        asyncio.create_task(uploadFileObj(file_bytes, S3_BUCKET, filename))

        return Response(f"{name_without_extension}:{sdb_result}", media_type="text/plain")

    except Exception as e:
        return Response(f"ERROR: {str(e)}", media_type="text/plain", status_code=500)
