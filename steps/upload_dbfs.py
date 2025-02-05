import requests
import json
import base64

# Replace these with your Databricks workspace details
DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]

# Local file path and DBFS destination path
LOCAL_FILE_PATH = "data/inputTest.csv"
DBFS_PATH = "/FileStore/inputTest.csv"

import time

def request_retry(method, url, headers=None, data=None, files=None, json=None, max_retries=20, delay=5):
    """
    Retry an API request in case of failure.

    Args:
        method (str): HTTP method (e.g., "POST", "GET").
        url (str): API endpoint URL.
        headers (dict): Request headers.
        data (dict): Form data for the request.
        files (dict): Files to upload.
        json (dict): JSON payload for the request.
        max_retries (int): Maximum number of retries.
        delay (int): Delay between retries in seconds.

    Returns:
        requests.Response: The response from the API.

    Raises:
        Exception: If max retries are reached and the request still fails.
    """
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, headers=headers, data=data, files=files, json=json)
            if response.status_code in [200, 201]:  # Success status codes
                return response
            else:
                print(f"Attempt {attempt + 1} failed with status code {response.status_code}. Retrying in {delay} seconds...")
                print(f"Response: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed with exception: {e}. Retrying in {delay} seconds...")
        
        time.sleep(delay)  # Wait before retrying

    raise Exception(f"Max retries ({max_retries}) reached. Request failed.")

# Step 1: Create the DBFS directory (if it doesn't exist)
def create_dbfs_directory(path):
    url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/mkdirs"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {"path": path}
    response = request_retry(method="POST", url=url, headers=headers, json=data )
    # response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        print(f"Directory created: {path}")
    else:
        print(f"Error creating directory: {response.text}")

# Step 2: Upload the file to DBFS
def upload_file_to_dbfs(local_path, dbfs_path):
    # Open the local file in binary mode
    with open(local_path, "rb") as file:
        file_content = file.read()

    # Encode the file content in base64
    file_content_base64 = base64.b64encode(file_content).decode("utf-8")

    # Upload the file to DBFS
    url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/put"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {
        "path": dbfs_path,
        "contents": file_content_base64,
        "overwrite": "true"  # Set to False if you don't want to overwrite existing files
    }
    response = request_retry(method="POST", url=url, headers=headers, json=data )
    # response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        print(f"File uploaded to DBFS: {dbfs_path}")
    else:
        print(f"Error uploading file: {response.text}")

# Create the DBFS directory (if needed)
create_dbfs_directory("/FileStore/")

# Upload the file
upload_file_to_dbfs(LOCAL_FILE_PATH, DBFS_PATH)