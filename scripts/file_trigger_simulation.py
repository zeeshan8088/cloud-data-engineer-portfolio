def on_file_upload(file_name):
    print(f"New file received: {file_name}")
    print("Validating file...")
    print("Loading data into BigQuery raw table...")
    print("Ingestion complete.")

# Simulate event
on_file_upload("orders_2024_01.csv")
