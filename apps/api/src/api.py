import boto3

s3 = boto3.client('s3')


def lambda_handler(event, context):
    print(event)
