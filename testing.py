import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor

# Configuration
API_URL = "http://127.0.0.1:8000/"
IMAGE_DIR = "/Users/gaurav/projects/cc/project1/facial_recognition/face_images_1000"
NUM_REQUESTS = 100  # Limit the number of requests
NUM_MAX_WORKERS = 100  # Number of concurrent threads

# Statistics
responses = 0
err_responses = 0
ex_requests = []

def send_one_request(image_path):
    """Send a single request with an image file."""
    global responses, err_responses, ex_requests

    file = {"inputFile": open(image_path, "rb")}
    try:
        response = requests.post(API_URL, files=file)
        if response.status_code != 200:
            print(f"sendErr: {response.url}")
            err_responses += 1
        else:
            responses += 1
    except requests.exceptions.RequestException as errex:
        print("Exception:", errex)
        ex_requests.append(image_path)
    finally:
        file["inputFile"].close()

def main():
    # Get list of image paths
    image_path_list = []
    for i, name in enumerate(os.listdir(IMAGE_DIR)):
        if i == NUM_REQUESTS:
            break
        image_path_list.append(os.path.join(IMAGE_DIR, name))

    # Start time
    start_time = time.time()

    # Execute requests in parallel
    with ThreadPoolExecutor(max_workers=NUM_MAX_WORKERS) as executor:
        executor.map(send_one_request, image_path_list)

    # End time
    end_time = time.time()
    total_time = round((end_time - start_time) * 1000)  # Convert to milliseconds

    # Print results
    print(f"Total Execution Time: {total_time}ms")
    print(f"Successful responses: {responses}")
    print(f"Failed responses: {err_responses}")
    print(f"Exceptions encountered: {len(ex_requests)}")

if __name__ == "__main__":
    main()
