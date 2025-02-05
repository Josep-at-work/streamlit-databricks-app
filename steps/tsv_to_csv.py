import pandas as pd

# Input and output file paths
input_file_path = "data/customertestdata.csv"  # Path to the tab-separated file
output_file_path = "data/inputTest2.csv"  # Path to save the converted CSV file

# Read the tab-separated file into a DataFrame
df = pd.read_csv(input_file_path, sep="\t")

# Save the DataFrame to a CSV file with proper quoting
df.to_csv(output_file_path, index=False, quoting=1)  # quoting=1 ensures strings with commas are quoted

print(f"File converted and saved to: {output_file_path}")