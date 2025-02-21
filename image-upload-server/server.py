import asyncio
import boto3
from fastapi import FastAPI, File, UploadFile, HTTPException, Response

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
    """Fetch attribute value from AWS SimpleDB asynchronously using boto3"""
    try:
        response = await asyncio.to_thread(
            sdb_client.get_attributes,
            DomainName=SDB_DOMAIN_NAME,
            ItemName=item_name
        )
        if "Attributes" in response:
            for attr in response["Attributes"]:
                if attr["Name"] == attribute_name:
                    return attr["Value"]
        return None
    except Exception as e:
        print(f"Error fetching attribute: {e}")
        return None


async def upload_to_s3(file_obj, bucket: str, key: str):
    """Upload file directly to S3 asynchronously using boto3"""
    try:
        await asyncio.to_thread(
                s3_client.put_object, Bucket=bucket, Key=key, Body=file_obj
            )
        return key
    except Exception as e:
        print(f"Upload failed for {key}: {e}")
        return None


@app.post("/")
async def upload_file(inputFile: UploadFile = File(...)):
    try:
        filename = inputFile.filename
        name_without_extension, _ = filename.rsplit(".", 1)

        # Fetch attribute from SimpleDB asynchronously
        sdb_result = await get_attribute_value(name_without_extension, "Results")

        # Upload file to S3 asynchronously without storing it locally
        s3_result = await upload_to_s3(inputFile.file, S3_BUCKET, filename)

        if s3_result is None:
            return Response(f"ERROR: Failed to upload {filename} to S3", media_type="text/plain", status_code=500)

        return Response(f"{name_without_extension}:{sdb_result}", media_type="text/plain")

    except Exception as e:
        return Response(f"ERROR: {str(e)}", media_type="text/plain", status_code=500)