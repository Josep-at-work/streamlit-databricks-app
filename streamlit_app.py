import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient #to execute notebooks from the UI
import requests
import base64
import json
import time
import gsfuntions as gs
import io

DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]

DBFS_FILE_DIR = "/FileStore/UItest/"  # Replace with the DBFS path you want to upload the data
MAPPING_FILE_PATH = "/FileStore/UItest/Mapping_to_DBFS.csv" # Replace with the DBFS path containing the file you want to read and edit
UPDATED_FILE_PATH = "/FileStore/UItest/Mapping_to_DBFS_UPADTED.csv"

# Streamlit UI
st.set_page_config(layout="centered", page_title="Data Editor", page_icon=":recycle:")
st.title("Upload CSV & Edit the data and store it in DBFS")

# Initialize session state variables
if "uploaded_file" not in st.session_state:
    st.session_state.uploaded_file = None
if "df" not in st.session_state:
    st.session_state.df = None
if "df_edit" not in st.session_state:
    st.session_state.df_edit = None
if "editing" not in st.session_state:
    st.session_state.editing = False  # Track whether we are in editing mode
    
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None and st.session_state.uploaded_file is None:
    
    # Store the uploaded file in session state
    st.session_state.uploaded_file = uploaded_file
    directory_exists = gs.create_dbfs_directory(DBFS_FILE_DIR, DATABRICKS_HOST, DATABRICKS_TOKEN)
    print("File uploded")
    if directory_exists:
        print("Directory created")
        dbfs_uploading_path = DBFS_FILE_DIR + uploaded_file.name
        dbfs_path = gs.upload_file_to_dbfs(uploaded_file, dbfs_uploading_path, DATABRICKS_HOST, DATABRICKS_TOKEN)
        
        if dbfs_path is not None:
            uploaded_file.seek(0) # Reset the file pointer to the beginning of the file to store it in a df
            st.success(f"File uploaded successfully to: {dbfs_path}")
            st.write("Preview of uploaded file:")
           
            # Read the uploaded file into a DataFrame and store it in session state
            st.session_state.df = pd.read_csv(uploaded_file)
            st.dataframe(st.session_state.df, hide_index=True, use_container_width=True)
        # Here we should add the module that is able to take the customer data and define the mapping.
        #instead a file with a wrong mapping has been saved. Visualize this file and edit it.

if st.session_state.df is not None and st.button("View Mapping", type="primary"):
    # Set editing mode to True
    st.session_state.editing = True
    
def update_df():
    st.session_state.df_edit = pd.DataFrame(st.session_state.data_editor)

if st.session_state.editing:
    if st.session_state.df_edit is None:
        # Read the output file from DBFS and store it in session state
        st.session_state.df_edit = gs.read_output_file(MAPPING_FILE_PATH, DATABRICKS_HOST, DATABRICKS_TOKEN)

    if st.session_state.df_edit is not None:
        st.write("You can edit the mapping bellow:")

        df_updated = st.data_editor(st.session_state.df_edit, hide_index=True, use_container_width=True, disabled=["Standard Structure Column"])
        
        if st.button("Validate Mapping", type="secondary"):
            dbfs_path = gs.upload_file_to_dbfs(df_updated, UPDATED_FILE_PATH, DATABRICKS_HOST, DATABRICKS_TOKEN)
            st.success(f"Changes saved to DBFS! {dbfs_path}")
            # Reset editing mode
            st.session_state.editing = False
            st.session_state.df_edit = None
            
        