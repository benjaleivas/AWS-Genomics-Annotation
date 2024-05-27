################################################################################
# SETUP
################################################################################

#Dependencies
import os
import sys
import ast
import json
import boto3
import logging
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone, timedelta

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_config.ini')

#Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

#AWS clients/resources
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html 
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/service-resource/get_queue_by_name.html
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
s3 = boto3.client("s3", region_name=config['aws']['AWS_REGION_NAME'])
sqs = boto3.client('sqs', region_name=config['aws']['AWS_REGION_NAME'])
glacier = boto3.client('glacier', region_name=config['aws']['AWS_REGION_NAME'])
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

################################################################################
# HELPER FUNCTIONS
################################################################################

def receive_message(): 
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/queue/receive_messages.html
    response = sqs.receive_message(
        QueueUrl=config['aws']['AWS_SQS_GLACIER_THAW_QUEUE_URL'], 
        MaxNumberOfMessages=1, 
        WaitTimeSeconds=20)
    try:
        logger.info(f"thaw response: {response}")
        message = response[0]
        message_body = ast.literal_eval(message.body)
        receipt_handle = message.receipt_handle
    except KeyError:
        return None, None
    return ast.literal_eval(message_body), receipt_handle

def get_job_output(job_id):
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
    response = glacier.get_job_output(
        vaultName=config['aws']['AWS_GLACIER_VAULT'],
        jobId=job_id)
    return response['body'].read()

def generate_s3_key(archive_id):
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
    try:
        response = table.query(
            IndexName='UserIdIndex',
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression="user_id, job_id, input_file_name, results_file_archive_id",
            KeyConditionExpression=Attr('results_file_archive_id').eq(archive_id)
        )
        if len(response['Items']) == 1:
            annotation = response['Items'][0]
            object_prefix = config['aws']['AWS_S3_KEY_PREFIX'].split('/')[0]
            user_id = annotation['user_id']
            job_id = annotation['job_id']
            input_file_name = annotation['input_file_name']
            results_file_name = input_file_name.rstrip('.vcf') + '.annot.vcf'
            s3_key_result_file = f'{object_prefix}/{user_id}/{job_id}/{results_file_name}'
            return s3_key_result_file, job_id
        elif len(response['Items']) == 0:
            logger.error('Error: 404 - Item not found')
        else:
            logger.error('Error: 500 - Server Error')
    except ClientError as e:
        logger.error(f'Error querying DynamoDB: {e}')
    except Exception as e:
        logger.error(f'Unexpected error: {e}')

    return None, None


def upload_to_s3(body_bytes, s3_key_result_file):
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
    try: 
        response = s3.put_object(
            Body=body_bytes, 
            Bucket=config['aws']['AWS_S3_RESULTS_BUCKET'], 
            Key=s3_key_result_file
        )
        return response
    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"Failed to upload to file to results bucket: {e}") 
        return None

def update_dynamodb(s3_key_result_file, job_id):
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
    try:             
        response = dynamodb.update_item(
            TableName=config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'], 
            Key={'job_id': job_id}, 
            ExpressionAttributeValues={
                ':f': s3_key_result_file
            }, 
            UpdateExpression='SET s3_key_result_file = :f REMOVE results_file_archive_id'
        )
        return response
    except exceptions.ClientError as e:
        logger.error(f"Client error: {e.response['Error']['Code']}")
        return None

def delete_message(receipt_handle): 
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
    try:
        response = sqs.delete_message(
            QueueUrl=config['aws']['AWS_SQS_GLACIER_THAW_QUEUE_URL'], 
            ReceiptHandle=receipt_handle)
    except exceptions.ClientError as e:
        logger.error(f"Couldn't delete message due to client error: {e.response['Error']['Code']}")

################################################################################
# MAIN
################################################################################

logger.info('Checking users to thaw data for...')
while True:
    response, receipt_handle = receive_message()
    if response is None:
        logger.info("No messages received in thaw queue.")
        continue
    logger.info(F"response: {response}")
    logger.info(F"receipt_handle: {receipt_handle}")
    if response == None:
        continue
    message_dict = json.loads(response['Message'])
    if message_dict['StatusCode'] != 'Succeeded': 
        print('Error: 500 - StatusCode was not <Succeeded>')
        continue
    job_id = message_dict['JobId']
    archive_id = message_dict['ArchiveId']
    body_bytes = get_job_output(job_id)
    s3_key_result_file, job_id = generate_s3_key_name(archive_id)
    if s3_key_result_file == None: 
        print('Error: 500 - Server Error')
        continue
    if upload_to_s3(body_bytes, s3_key_result_file) == None: 
        continue
    if update_dynamodb(s3_key_result_file, job_id) == None: 
        continue
    delete_message(receipt_handle)

### EOF