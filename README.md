# streamlit-databricks-app 

[My Streamlit Databricks APP](https://app-databricks-app-5kglbs68nczgg5ufregvqt.streamlit.app/)

App Workflow:

1. Upload a CSV file.
2. Visualize the data.
3. Store this file into DBFS.
4. Visualize a Mapping of the standarized Vs customer attributes.(This mapping is currently added by me).
5. Edit the mapping from the UI.
6. Validate and store back the new version of the mapping.


... working in pogress

Next steps

1. Create the paths automatically so that are not overwriten every time.
2. Develop step 3 solution so that the Mapping_to_DBFS.csv is not uploaded manually in dataricks.
3. Allow for more than one file to be uploaded. This requires to edit the conditions of the main page. 
4. Validate if there are discrepancies between the standard data type attributes and the mapping data type proposed by the customer to raise a warnign.
5. Make it pretty.
