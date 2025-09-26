import boto3
import os

def set_aws_creds():
    
    session = boto3.Session()
    credentials = session.get_credentials()
    
    if not credentials:
        raise Exception("No AWS credentials found. Make sure you're logged in via AWS CLI/SSO.")

    frozen_creds = credentials.get_frozen_credentials()
   
    os.environ['AWS_ACCESS_KEY_ID'] = frozen_creds.access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = frozen_creds.secret_key
    if frozen_creds.token:
        os.environ['AWS_SESSION_TOKEN'] = frozen_creds.token
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    
    return session, credentials