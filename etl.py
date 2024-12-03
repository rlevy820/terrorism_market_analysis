from google.cloud import storage
import pandas as pd
import os

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

# Replace with your bucket name
BUCKET_NAME = ""

def list_bucket_contents(bucket_name):
    try:
        # Initialize the client
        client = storage.Client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # List files in the bucket
        blobs = bucket.list_blobs()

        print(f"Contents of bucket '{bucket_name}':")
        for blob in blobs:
            print(f"- {blob.name}")

    except Exception as e:
        print(f"An error occurred: {e}")

def clean_sp500_data(file_path, output_path):
    try:
        # Load the raw data
        sp500 = pd.read_csv(file_path)
        
        # Convert 'Date' column to datetime format
        sp500['Date'] = pd.to_datetime(sp500['Date']).dt.strftime('%m/%d/%y')
        
        # Remove commas from numeric columns and convert to float
        numeric_columns = ['Price', 'Open', 'High', 'Low']
        for col in numeric_columns:
            sp500[col] = sp500[col].str.replace(',', '').astype(float)
        
        # Convert 'Change %' column to float
        sp500['Change %'] = sp500['Change %'].str.replace('%', '').astype(float)
        
        # Drop the 'Vol.' column
        if 'Vol.' in sp500.columns:
            sp500 = sp500.drop(columns=['Vol.'])
        
        # Save cleaned data to a new file
        sp500.to_csv(output_path, index=False)
        print(f"Cleaned S&P data saved to {output_path}")

    except Exception as e:
        print(f"An error occurred while cleaning the data: {e}")


def clean_terrorism_data(file_path, output_path):
    try:
        # Load the raw data
        terrorism = pd.read_csv(file_path)
        
        # Create the 'Date' column by combining 'iyear', 'imonth', 'iday'
        terrorism['Date'] = pd.to_datetime(
            terrorism[['iyear', 'imonth', 'iday']]
            .astype(str)
            .agg('-'.join, axis=1),
            errors='coerce'
        ).dt.strftime('%m/%d/%y')
        
        # Drop the original 'iyear', 'imonth', and 'iday' columns
        terrorism = terrorism.drop(columns=['iyear', 'imonth', 'iday'])

        # Calculate the percentage of missing values in each column
        missing_percent = terrorism.isna().mean() * 100
        print("Percentage of missing values per column:")
        print(missing_percent)
        
        # Drop columns with more than 50% missing values
        columns_to_drop = missing_percent[missing_percent > 50].index
        terrorism = terrorism.drop(columns=columns_to_drop)
        print(f"Columns dropped: {list(columns_to_drop)}")
        
        # Drop rows with any remaining missing values
        terrorism = terrorism.dropna()
        print(f"Number of rows after removing rows with NA: {len(terrorism)}")
        
        # Save cleaned data to a new file
        terrorism.to_csv(output_path, index=False)
        print(f"Cleaned Terrorism data saved to {output_path}")

    except Exception as e:
        print(f"An error occurred while cleaning the data: {e}")


def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    try:
        # Initialize the client
        client = storage.Client()

        # Get the bucket
        bucket = client.bucket(bucket_name)

        # Upload the file
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)

        print(f"File {source_file} uploaded to {bucket_name}/{destination_blob_name}.")

    except Exception as e:
        print(f"An error occurred during upload: {e}")

if __name__ == "__main__":
    # List bucket contents
    list_bucket_contents(BUCKET_NAME)

    # Clean and upload the S&P dataset
    sp500_input_file = "sp500.csv"  # Replace with your input file name
    sp500_output_file = "sp500_cleaned.csv"  # Replace with your desired output file name
    clean_sp500_data(sp500_input_file, sp500_output_file)
    sp500_blob_name = "cleaned_data/sp500_cleaned.csv"
    upload_to_gcs(BUCKET_NAME, sp500_output_file, sp500_blob_name)

    # Clean and upload the Terrorism dataset
    terrorism_input_file = "terrorism.csv"  # Replace with your input file name
    terrorism_output_file = "terrorism_cleaned.csv"  # Replace with your desired output file name
    clean_terrorism_data(terrorism_input_file, terrorism_output_file)
    terrorism_blob_name = "cleaned_data/terrorism_cleaned.csv"
    upload_to_gcs(BUCKET_NAME, terrorism_output_file, terrorism_blob_name)
