import requests
import json
import base64
import time

DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]

dbfs_uploading_path = "/FileStore/UItest/"
# Notebook path and parameters
NOTEBOOK_PATH = "/Workspace/Users/josepbasketarta@gmail.com/gs-ui-v1/gs-ui-app_2025_01_30/MappingTestv1"
CLUSTER_ID = "0130-102203-h8ikvfn7"
DBFS_FILE_PATH = dbfs_uploading_path + "inputTest2.csv"

def trigger_databricks_notebook(notebook_path, dbfs_file_path):
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

def check_notebook_status(run_id):
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
                break
            else:
                print(f"Notebook execution status: {status}")
                time.sleep(10)  # Wait for 10 seconds before checking again
        else:
            print(f"Error checking notebook status: {response.text}")
            break

# Trigger the notebook execution
run_id = trigger_databricks_notebook(NOTEBOOK_PATH, DBFS_FILE_PATH)

if run_id:
    # Check the status of the notebook execution
    check_notebook_status(run_id)