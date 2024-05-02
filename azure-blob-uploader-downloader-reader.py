from azure.storage.blob import BlobClient
import os
import pandas as pd

def load_env_vars():
    """
    Loads required environment variables and checks their presence.
    Raises an error if any are missing.
    """
    sas_token = os.getenv('STORAGE_SAS_TOKEN_CHD')
    container_name = os.getenv('CONTAINER_NAME_CHD')
    storage_account = os.getenv('STORAGE_ACCOUNT_CHD')
    
    if not all([sas_token, container_name, storage_account]):
        raise EnvironmentError("One or more required environment variables are missing.")
    
    return sas_token, container_name, storage_account

def upload_file(sas_token, container_name, storage_account, local_file_path, blob_path):
    """
    Uploads a single file from 'local_file_path' to 'blob_path' in Azure Blob Storage.
    """
    sas_url = f"https://{storage_account}.blob.core.windows.net/{container_name}/{blob_path}?{sas_token}"
    blob_client = BlobClient.from_blob_url(blob_url=sas_url)
    try:
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"Upload completed successfully for {blob_path}!")
    except Exception as e:
        print(f"Failed to upload {blob_path}: {e}")

def upload_from_path(sas_token, container_name, storage_account, local_path, blob_base_path):
    """
    Recursively uploads files from 'local_path' to Azure Blob Storage under 'blob_base_path'.
    Handles both single files and directories including nested directories.
    """
    if os.path.isfile(local_path):
        # Directly use blob_base_path as the blob_path for files
        upload_file(sas_token, container_name, storage_account, local_path, blob_base_path)
    elif os.path.isdir(local_path):
        # It's a directory, recursively upload all contents
        for item in os.listdir(local_path):
            item_full_path = os.path.join(local_path, item)
            item_blob_path = os.path.join(blob_base_path, item)
            upload_from_path(sas_token, container_name, storage_account, item_full_path, item_blob_path)

def download_file(sas_token, container_name, storage_account, blob_path, local_file_path):
    sas_url = f"https://{storage_account}.blob.core.windows.net/{container_name}/{blob_path}?{sas_token}"
    blob_client = BlobClient.from_blob_url(blob_url=sas_url)
    try:
        download_stream = blob_client.download_blob()
        content = download_stream.readall()
        if not content:
            print(f"The blob at {blob_path} is empty or could not be read.")
            return
        with open(local_file_path, "wb") as data:
            data.write(content)
            print(f"Download completed successfully for {blob_path}!")
    except Exception as e:
        print(f"Failed to download {blob_path}: {e}")
        # Print exception details and possibly the response status
        if hasattr(e, 'response'):
            print(f"HTTP status code: {e.response.status_code}")

def read_and_print_blob(sas_token, container_name, storage_account, blob_path):
    """
    Reads data from a blob into a DataFrame and prints it.
    Assumes the blob contains data in a format that pandas can read, like CSV.
    """
    sas_url = f"https://{storage_account}.blob.core.windows.net/{container_name}/{blob_path}?{sas_token}"
    blob_client = BlobClient.from_blob_url(blob_url=sas_url)
    try:
        download_stream = blob_client.download_blob()
        content = download_stream.readall()
        if not content:
            print(f"The blob at {blob_path} is empty or could not be read.")
            return
        # Assuming the file is in CSV format
        from io import StringIO
        data_str = StringIO(content.decode('utf-8'))
        df = pd.read_csv(data_str)
        print(df)
    except Exception as e:
        print(f"Failed to read data from {blob_path}: {e}")

def main():
    """
    Main function to handle environment variable loading and decide whether to upload, download, or read data based on user input.
    """
    sas_token, container_name, storage_account = load_env_vars()

    choice = input("Choose an option:\n1. Upload File\n2. Download File\n3. Read and Print File\nEnter choice (1, 2, or 3): ")
    
    if choice == '1':
        local_path = os.path.expanduser('~/Downloads/test-may-2.csv')
        blob_base_path = "test-may-2.csv"
        upload_from_path(sas_token, container_name, storage_account, local_path, blob_base_path)
    elif choice == '2':
        local_path = os.path.expanduser('~/Downloads/downloaded-test-may-2.csv')
        blob_path = "test-may-2.csv"
        download_file(sas_token, container_name, storage_account, blob_path, local_path)
    elif choice == '3':
        blob_path = "test-may-2.csv"  # Make sure this is the correct path to a readable file
        read_and_print_blob(sas_token, container_name, storage_account, blob_path)
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()

