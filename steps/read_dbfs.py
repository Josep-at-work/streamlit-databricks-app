import requests
import json
import base64
import time
import pandas as pd
from io import StringIO

DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]
OUTPUT_FILE_PATH = "/FileStore/UItest/output_mapped.csv"

def read_output_file(output_path):
    """
    Read the output file from DBFS into a Pandas DataFrame.
    """
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/read"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {"path": output_path}
    response = requests.get(url, headers=headers, json=data)
    if response.status_code == 200:
        response_json = response.json() # Convert the respons to a json and sotore ir in a new json variable
        base64_string = response_json["data"]  # Extract the Base64-encoded string

        # Decode Base64 content to get the original file data
        decoded_bytes = base64.b64decode(base64_string)
        
        # Convert bytes to a readable format (string)
        decoded_str = decoded_bytes.decode("utf-8")
        df = pd.read_csv(StringIO(decoded_str))
        # with open("temp.csv", "r", encoding="utf-8") as f:
        #     data = json.loads(f.read())
        
        # df = pd.json_normalize(data)
        print(df.head())

        # with open("temp.csv", "rb") as f:
        #     encoded_content = base64.b64decode(f.read()).decode("utf-8")

        # with open("encoded_temp.csv", "w", encoding="utf-8") as f:
        #     f.write(encoded_content)
        
        # df = pd.read_csv("encoded_content.csv")

        # # Save the file locally in a temp file in disk
        # with open("temp.csv", "wb") as f:
        #     f.write(response.content)
        # # Read the file into a DataFrame
        # df = pd.read_csv("temp.csv", encoding="ISO-8859-1")
        # print(df.head())
        # return df
    else:
        print(f"Error reading output file: {response.text}")
        return None

try:
    df_output = read_output_file(OUTPUT_FILE_PATH)
    if df_output is not None:
        # print(df_output)
        print("end without error")
except Exception as e:
     print(f"Error reading data: {e}")

# if st.button("Preview Output"):

# Read the output file from DBFS
# df_output = read_output_file(OUTPUT_FILE_PATH)
# if df_output is not None:
#     st.write("Preview of the output file:")
#     st.dataframe(df_output)
# else:
#     st.error("Failed to read the output file.")