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
config.read('restore_config.ini')

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
sqs = boto3.resource('sqs', region_name=config['aws']['AWS_REGION_NAME'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['AWS_SQS_GLACIER_RESTORE_QUEUE_NAME'])
glacier = boto3.client('glacier', region_name=config['aws']['AWS_REGION_NAME'])
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

################################################################################
# HELPER FUNCTIONS
################################################################################

def query_user_archive_ids(user_id): 
    """
    """
    try:
        #Query table to get the job IDs
        #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        response = table.query(
            IndexName='UserIdIndex',
            ProjectionExpression="job_id", 
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        job_ids = [item['job_id'] for item in response['Items']]
        archive_ids = []
        for job_id in job_ids:
            #Query table to get required attributes
            #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
            job_response = table.get_item(
                Key={'job_id': job_id},
                ProjectionExpression="results_file_archive_id, s3_key_result_file"
            )
            job_item = job_response.get('Item', {})
            if 'results_file_archive_id' in job_item and 's3_key_result_file' not in job_item:
                archive_ids.append(job_item)
        return archive_ids

    except ClientError as e: 
        logger.error(f"Could not retrieve archive ids for user {user_id}: {e}")

def start_retrieval_job(archive_id, tier='Expedited'):
    """
    """
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
    try:
        logger.info(f"Initializing {tier} job for ArchiveId: {archive_id}")
        glacier.initiate_job(
            vaultName=config['aws']['AWS_GLACIER_VAULT'],
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'SNSTopic': config['aws']['AWS_SNS_THAW_TOPIC_ARN'],
                'Tier': tier
            }
        )
    except glacier.exceptions.InsufficientCapacityException:
        logger.warning(f"Insufficient capacity for {tier} retrieval. Trying Standard tier...")
        start_retrieval_job(archive_id, tier='Standard')
    except ClientError as e:
        logger.error(f"Failed to initiate job for ArchiveId {archive_id}: {e}")

def process_message(user_id):
    """
    """
    try:
        archive_ids = query_user_archive_ids(user_id)
        for item in archive_ids:
            archive_id = item['results_file_archive_id']
            start_retrieval_job(archive_id)
    except Exception as e:
        logger.error(f"Couldn't process message for user_id {user_id}: {e}")

def delete_sqs_message(message):
    """
    """
    try:
        message.delete()
        logger.info('Message deleted from queue.')
    except ClientError as e:
        logger.error(f'Client Error: {e}')

################################################################################
# MAIN 
################################################################################

logger.info('Checking users to restore data for...')
while True: 
    #Poll queue for messages
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/queue/receive_messages.html
    response = queue.receive_messages(
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20
    )
    if not response:
        logger.info("No messages received in restore queue.")
        continue
    #Extract message
    try:
        logger.info("Beggining restore process.")
        message = response[0]
        message_body = ast.literal_eval(message.body)
        user_id = message_body.get('user_id')
        receipt_handle = message.receipt_handle
        if not user_id:
            logger.error("KeyError: user_id not found in message")
            continue
    except Exception as e:
        logger.error(f"Failed to extract message body: {e}")
    #Process message
    try:
        process_message(user_id)
        delete_sqs_message(message)            
    except ClientError as e:
        logger.error(f'ClientError while receiving messages from SQS: {e}')
    except Exception as e:
        logger.error(f'Unexpected error: {e}')

### EOF