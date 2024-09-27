import json
import boto3
import re

s3 = boto3.client('s3')

# Define retention periods in days based on folder names
RETENTION_MAP = {
    '7days': 7,
    '1month': 30,
    '3months': 90,
    '3month': 90,
    '5months': 150,
    '1year': 365,
    '3year': 1095,
    '3years': 1095,
    '5year': 1825,
    '5years': 1825,
    '7year': 2555,
    '7years': 2555
}

lifecycle_policy = {'Rules': [],}

def lambda_handler(event, context):
    # List all S3 buckets
    # response = s3.list_buckets()
    # buckets = response['Buckets']
    buckets =  [{'Name': 'dang-le-s3-object', 'CreationDate': 10}, {'Name': 'amplify-demoapp-staging-141737-deployment', 'CreationDate': 20}]
    
    for bucket in buckets:
        bucket_name = bucket['Name']
        
        # List the top-level folders in each bucket
        top_level_folders = list_top_level_folders(bucket_name)
        
        for folder in top_level_folders:
            if folder in RETENTION_MAP.keys():
                # Check if lifecycle policy already exists
                if not check_existing_policy(bucket_name, folder):
                    # Create lifecycle policy for the folder
                    lifecycle_policy["Rules"].append(
                        {
                            'Expiration': {
                                'Days': RETENTION_MAP[folder],
                            },
                            'Filter': {
                                'And':
                                    {
                                    'Prefix': f"{folder}/",
                                    'Tags': [{
                                        'Key': 'isFile',
                                        'Value': 'true'
                                    }],
                                }
                            },
                            'ID': f"{folder}-retention-policy",
                            'Status': 'Enabled',
                        },
                    )
                    # create_lifecycle_policy(bucket_name, folder, RETENTION_MAP[folder])
                else:
                    print(f"Lifecycle policy already exists for {bucket_name}/{folder}, skipping...")
    
        s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_policy)
        
        #Clean Rules for new bucket
        lifecycle_policy["Rules"].clear()
 
    return {
        'statusCode': 200,
        'body': json.dumps('Lifecycle policies applied where necessary')
    }

def list_top_level_folders(bucket_name):
    # List objects in the bucket, simulating top-level folder check
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    
    # Extract folders (prefixes) from the response
    if 'CommonPrefixes' in response:
        return [prefix['Prefix'].rstrip('/') for prefix in response['CommonPrefixes']]
    else:
        return []

def check_existing_policy(bucket_name, folder):
    try:
        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        # Check if a rule for the folder already exists
        for rule in response['Rules']:
            if rule.get('Filter', {}).get('Prefix') == f'{folder}/' and rule['Status'] == 'Enabled':
                return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return False  # No lifecycle policy exists
        else:
            raise
    return False