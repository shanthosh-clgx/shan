import pandas as pd

file_path = "/mnt/c//Users/wcheng/Downloads/yellow_tripdata_2025-01.parquet"  # Replace with the actual path to your file
df = pd.read_parquet(file_path)

print(df.head())

df.info()
