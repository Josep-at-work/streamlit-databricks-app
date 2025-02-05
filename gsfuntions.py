import requests
import base64
import json
from databricks.sdk import WorkspaceClient #to execute notebooks from the UI
import time
import pandas as pd
from io import StringIO

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
def create_dbfs_directory(path, DATABRICKS_HOST, DATABRICKS_TOKEN):
    """
     Creates a file directory (if not exists already) REST API.
     
     Args:
        path (str): Path of the directoy in the Databricks dbfs.
     Return:
        (Boolean) Weather the request has been completed successfully. 
    """
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/mkdirs"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {"path": path}
    response = request_retry(method="POST", url=url, headers=headers, json=data )
    if response.status_code == 200:
        print(f"Directory created: {path}")
        return True
    else:
        print(f"Error creating directory: {response.text}")
        return False

# Step 2: Upload the file to DBFS
def upload_file_to_dbfs(file, dbfs_path, DATABRICKS_HOST, DATABRICKS_TOKEN):
    """
    Uploades a file to DBFS via REST API. If it already exists, the file is overwitten.

    Args:
        file (file object): File object.
        dbfs_path (str): Path to upload the file without the "dbfs:" part of the path.
            For example for the file in dbfs:/FileStore/UItest/inputTest2.csv, dbfs_path is FileStore/UItest/inputTest2.csv. 
    Return:
        (str) Path name. 
    """
    if isinstance(file, pd.DataFrame):
        print("file is a Pandas DataFrame")
        # Convert the DataFrame to a CSV string
        string = file.to_csv(index=False)
        file_content = string.encode("utf-8") #convert to bytes 
        
    else:
        print("file is NOT a Pandas DataFrame")
        file_content = file.read()
    
    # Encode the file content in base64, required for the databricks REST API
    file_content_base64 = base64.b64encode(file_content).decode("utf-8")

    # Upload the file to DBFS
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {
        "path": dbfs_path,
        "contents": file_content_base64,
        "overwrite": "true"  # Set to False if you don't want to overwrite existing files
    }
    response = request_retry(method="POST", url=url, headers=headers, json=data )
    if response.status_code == 200:
        print(f"File uploaded to DBFS: {dbfs_path}")
        return f"dbfs:{dbfs_path}"
    else:
        print(f"Error uploading file: {response.text}")

# Step 3: Trigger the mapping
def trigger_databricks_notebook(notebook_path, dbfs_file_path, DATABRICKS_HOST, DATABRICKS_TOKEN, CLUSTER_ID):
    """
    Trigger the execution of a Databricks notebook using the REST API.

    Args:
        notebook_path (str): Path to the notebook in the Databricks workspace.
        dbfs_file_path (str): Path to the file in DBFS to pass as a parameter.
    """
    # API endpoint to run a notebook
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"

    # Request payload
    payload = {
        "run_name": "gs-ui-app-notebook-execution",
        "existing_cluster_id": CLUSTER_ID,  # Replace with your cluster ID
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "dbfs_file_path": dbfs_file_path  # Pass the DBFS file path as a parameter
            }
        }
    }

    # Headers for the API request
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Submit the job
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        run_id = response.json()["run_id"]
        print(f"Notebook execution started successfully. Run ID: {run_id}")
        return run_id
    else:
        print(f"Error triggering notebook execution: {response.text}")
        return None


def check_notebook_status(run_id, DATABRICKS_HOST, DATABRICKS_TOKEN):
    """
    Check the status of a Databricks notebook run.

    Args:
        run_id (str): The run ID of the notebook execution.
    """
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            status = response.json()["state"]["life_cycle_state"]
            if status in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = response.json()["state"]["result_state"]
                print(f"Notebook execution completed with status: {result_state}")
                return result_state
                break
            else:
                print(f"Notebook execution status: {status}")
                time.sleep(10)  # Wait for 10 seconds before checking again
        else:
            print(f"Error checking notebook status: {response.text}")
            break
            
# Step 4: Read the output from DBFS
def read_output_file(output_path, DATABRICKS_HOST, DATABRICKS_TOKEN ):
    """
    Read the output file from DBFS into a Pandas DataFrame.
    Return:
        Pandas dataframe with file content.
    """
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/read"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {"path": output_path}
    response = requests.get(url, headers=headers, json=data)
    if response.status_code == 200:
        response_json = response.json() # Convert the respons to a json and store ir in a new json variable
        base64_string = response_json["data"]  # Extract the Base64-encoded string

        # Decode Base64 content to get the original file data
        decoded_bytes = base64.b64decode(base64_string)
        
        # Convert bytes to a readable format (string)
        decoded_str = decoded_bytes.decode("utf-8")
        df = pd.read_csv(StringIO(decoded_str))
        return df
    else:
        print(f"Error reading output file: {response.text}")
        return None