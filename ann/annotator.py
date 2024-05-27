################################################################################
# SETUP
################################################################################

# Dependencies
import os
import sys
import json
import boto3
import driver
import logging
import subprocess
from configparser import ConfigParser
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Get configuration
config = ConfigParser(os.environ)
config.read('ann_config.ini')

#Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

#Boto3 S3 client, table
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html 
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
client = boto3.client("s3", region_name=config['aws']['AWS_REGION_NAME']) 
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

#Connect to SQS and get the message queue
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/service-resource/get_queue_by_name.html
sqs = boto3.resource("sqs", region_name=config['aws']['AWS_REGION_NAME'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['AWS_SQS_JOB_REQUEST_QUEUE_NAME'])

################################################################################
# MAIN
################################################################################

logger.info('Checking for annotation requests...')
while True:
    # Attempt to read a message from the queue
    try:
        messages = queue.receive_messages(WaitTimeSeconds=20)
        logger.info("Message received from job requests queue.")
    except Exception as e:
        logger.error(f"No messages received from queue: {e}")
    # Extract job parameters
    for message in messages:
        try:
            body = json.loads(message.body)
            data = json.loads(body["Message"])
            user_id = data["user_id"]
            user_name = data["user_name"]
            user_email = data["user_email"]
            user_role = data["user_role"]
            s3_key = data["s3_key"]
            job_id = data["job_id"]
            input_file_name = data["input_file_name"]
            bucket_name = data["bucket_name"]
            submit_time = data["submit_time"]
            job_status = data["job_status"]
        except Exception as e:
            logger.error(f"Failed to retrieve job parameters from message body: {e}")
        #Create job directory
        try:
            job_dir = f"../jobs/{job_id}"
            os.makedirs(job_dir, exist_ok=True)
            local_file_path = os.path.join(job_dir, input_file_name)
        except Exception as e:
            logger.error(f"Failed to create job directory: {e}")
        #Download input file
        #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
        try:
            client.download_file(bucket_name, s3_key, local_file_path)
            logger.info("Input file successfully downloaded from bucket.")
        except NoCredentialsError as e:
            logger.error(f"Failed to download file from S3 bucket. No credentials error: {e}")
        except PartialCredentialsError:
            logger.error(f"Failed to download file from S3 bucket. Partial credentials error: {e}")
        except ClientError as e:
            logger.error(f"Failed to download file from S3 bucket. Client error: {e}")
        #Launch annotation job as a background process
        #SOURCE: https://docs.python.org/3/library/subprocess.html#subprocess.Popen
        try:
            subprocess.Popen(["python", "run.py", local_file_path])
        except subprocess.CalledProcessError as e:
            logger.error(f"Annotation process failed with return code {e.returncode}: {e}")
        except FileNotFoundError as e:
            logger.error(f"Annotation script not found: {e}")
        except OSError as e:
            logger.error(f"OS error during annotation process: {e}")
        except Exception as e:
            logger.error(f"Unexpected error running job {job_id}: {e}")
        #Delete message
        try:
            message.delete()
            logger.info("Annotation message deleted.")
        except Exception as e:
            logger.error(f"Failed to delete message from SQS: {e}")


