import streamlit as st
import pandas as pd
import requests
import base64
import json
from io import StringIO

DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]

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



DBFS_FILE_DIR = "/FileStore/UItest/"  # Replace with the DBFS path you want to upload the data
OUTPUT_FILE_PATH = "/FileStore/UItest/Mapping_to_DBFS.csv" # Replace with the DBFS path containing the file you want to read and edit

st.set_page_config(layout="centered", page_title="Data Editor", page_icon="🧮")

st.title("✍️ Annotations")
st.caption("This is a demo of the `st.data_editor`.")

st.write("")

"""The new data editor makes it so easy to annotate data! Can you help us annotate sentiments for tweets about our latest release?"""

data = read_output_file(OUTPUT_FILE_PATH, DATABRICKS_HOST, DATABRICKS_TOKEN)

df = pd.DataFrame(data)
annotated = st.data_editor(df, hide_index=True, use_container_width=True, disabled=["Standard Structure Column"])
