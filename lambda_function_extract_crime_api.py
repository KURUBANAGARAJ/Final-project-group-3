import json
import boto3
import pandas as pd
from sodapy import Socrata

def lambda_handler(event, context):
    # Set up Socrata client
    client = Socrata("data.cityofchicago.org", None)

    # Fetch data from the API
    results = client.get("8i6r-et8s", limit=2000)

    # Convert data to DataFrame
    df = pd.DataFrame.from_records(results)

    # Write DataFrame to CSV
    csv_filename = '/tmp/chicago_school_api_data.csv'
    df.to_csv(csv_filename, index=False)

    # Upload CSV file to S3
    s3 = boto3.client('s3')
    bucket_name = 'final-project-group3-data'
    s3_key = 'data-LZ2/chicago_school_api_data.csv'
    s3.upload_file(csv_filename, bucket_name, s3_key)

    # Invoke the second Lambda function
    lambda_client = boto3.client('lambda')
    function_name = 'merge-school-and-crime-data'  # Name of the second Lambda function
    payload = {}  # Payload to send to the second Lambda function
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',  # Synchronous invocation
        Payload=json.dumps(payload)
    )

    # Check if the invocation was successful
    if response['StatusCode'] == 200:
        return {
            'statusCode': 200,
            'body': 'CSV file uploaded to S3 and second Lambda function invoked successfully'
        }
    else:
        return {
            'statusCode': response['StatusCode'],
            'body': 'Error invoking second Lambda function'
        }
