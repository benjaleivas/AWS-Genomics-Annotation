################################################################################
# SETUP
################################################################################

#Dependencies
import os
import sys
import time
import json
import boto3
import logging
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta

#Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

#Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_config.ini')

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
queue = sqs.get_queue_by_name(QueueName=config['aws']['AWS_SQS_GLACIER_ARCHIVE_QUEUE_NAME'])
glacier = boto3.client('glacier', region_name=config['aws']['AWS_REGION_NAME'])
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

################################################################################
# MAIN 
################################################################################

logger.info('Checking users to archive data for...')
while True:
    try:
        #Poll queue
        #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/queue/receive_messages.html
        response = queue.receive_messages(
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        if not response:
            logger.info("No messages received in archive queue.")
            continue
        #If messages were received
        for message in response:
            message_body = json.loads(message.body)
            notification_message = json.loads(message_body['Message'])
            #Extract relevant job parameters
            job_id = notification_message.get('job_id')
            s3_key = notification_message.get('s3_key')
            result_file = notification_message.get('result_key')
            complete_time = notification_message.get('complete_time')
            #Evaluate vault threshold
            current_time = int(time.time())
            time_since_completion = current_time - complete_time
            threshold = int(config['aws']['FREE_USER_DATA_RETENTION'])
            if time_since_completion >= threshold:
                logger.info("Initiating archival process.")
                #Download data
                #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
                try:
                    bucket = config['aws']['AWS_S3_RESULTS_BUCKET']
                    key = s3_key.split('~')[0]+'/'+result_file
                    filename = f"/tmp/{os.path.basename(result_file)}"
                    s3 = boto3.client("s3")
                    s3.download_file(
                        Bucket=bucket,
                        Key=key,
                        Filename=filename
                    )
                    with open(filename, 'rb') as f:
                        downloaded_data = f.read()
                    logger.info(f"\'{result_file}\' successfully downloaded from \'{bucket}\' bucket.")
                except Exception as e:
                    logger.error(f"Couldn't download file from \'{bucket}\' bucket: {e}")
                #Send to vault
                #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
                try:
                    s3 = boto3.client("s3")
                    response = glacier.upload_archive(
                        vaultName=config['aws']['AWS_GLACIER_VAULT'],
                        body=downloaded_data
                    )
                    logger.info(f"\'{result_file}\' sent to Glacier vault.")
                except Exception as e:
                    logger.error(f"Couldn't send \'{result_file}\' to Glacier vault: {e}")
                #Persist archive_id to DynamoDB, delete s3_key_result_file
                #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                try:
                    results_file_archive_id = response.get('archiveId')
                    table.update_item(
                        Key={"job_id": job_id},
                        UpdateExpression="""
                            SET results_file_archive_id = :archiveid
                            REMOVE s3_key_result_file
                        """,
                        ExpressionAttributeValues={
                            ":archiveid": results_file_archive_id
                        },
                    )
                    logger.info("ArchiveID persisted to DynamoDB, s3_key_result_file deleted.")
                except Exception as e:
                    logger.error(f"Couldn't persist 'archiveId' to DynamoDB: {e}")
                #Delete the file from the S3 bucket
                #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
                try:
                    s3 = boto3.client("s3")
                    s3.delete_object(
                        Bucket=config['aws']['AWS_S3_RESULTS_BUCKET'], 
                        Key=key
                        )
                    logger.info(f"Deleted {result_file} from S3 bucket.")
                except Exception as e:
                    logger.error(f"Couldn't delete results file from bucket: {e}")
                #Delete message
                try:
                    message.delete()
                    logger.info("Archive message successfully deleted.")
                except ClientError as e:
                    logger.error(f"Failed to delete message: {e}")
                    print(f"Failed to delete message: {e}")
    except ClientError as e:
        logger.error(f"Failed to poll the queue for messages: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
