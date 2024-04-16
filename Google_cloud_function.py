import json
from google.cloud import storage 

def process_json(event,context):
    """Cloud Function to process JSON data in a specific folder in a Cloud Storage bucket."""

    # Specify the folder you want to process
    target_folder = "raw_data_statistics/"
    print(event)
    # Extract bucket and file information from the event
    bucket_name = event['bucket']
    file_name = event['name']
    input_gcs_bucket = f"{bucket_name}"
    input_gcs_file = f"{file_name}"
    output_gcs_bucket = f"{bucket_name}"
    output_gcs_file = f"{file_name}"

    # Check if the file is within the target folder
    if file_name.startswith(f"{target_folder}"):
        # Create a storage client
        storage_client = storage.Client()
        #clean json data
        convert_and_upload_to_gcs(input_gcs_bucket, input_gcs_file, output_gcs_bucket, output_gcs_file)


        # Get the JSON file from Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        json_data = json.loads(blob.download_as_text())

        # Apply data cleansing logic (replace this with your actual logic)
        cleansed_data = cleanse_json(json_data)

        # Save the cleansed data back to Cloud Storage
        new_blob = bucket.blob(f"cleansed_{file_name}")
        new_blob.upload_from_string(json.dumps(cleansed_data))

        print(f"Data cleansing complete. Cleansed data saved to {new_blob.name}")
    else:
        print(f"File {file_name} is not in the target folder. Skipping processing.")
    
def cleanse_json(json_data):
    """Replace this function with your actual data cleansing logic."""
    # Example: Remove keys with null values
    return json_data["items"]

def convert_and_upload_to_gcs(input_bucket, input_file, output_bucket, output_file):
    # Create a storage client
    storage_client = storage.Client()

    # Get the JSON file from the input GCS bucket
    input_bucket_obj = storage_client.bucket(input_bucket)
    blob = input_bucket_obj.blob(input_file)
    json_data = json.loads(blob.download_as_text())

    # Convert JSON to a single line
    json_data_single_line = json.dumps(json_data, separators=(',', ':'))

    # Upload the single-line JSON to the output GCS bucket
    output_bucket_obj = storage_client.bucket(output_bucket)
    output_blob = output_bucket_obj.blob(output_file)
    output_blob.upload_from_string(json_data_single_line)
