################################################################################
# SETUP
################################################################################

# Dependencies
import os
import sys
import time
import json
import boto3
import driver
import shutil
import logging
from configparser import ConfigParser
# from util.helpers import send_email_ses
from botocore.exceptions import ClientError

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

#S3 client, table
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html 
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html
#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client('sns', region_name=config['aws']['AWS_REGION_NAME'])
table = dynamodb.Table(config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

#Bucket
s3_outputs_bucket = config['aws']['AWS_S3_RESULTS_BUCKET']

################################################################################
# TIMER CLASS
################################################################################

"""A rudimentary timer for coarse-grained profiling
"""

class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

################################################################################
# HELPER FUNCTIONS
################################################################################

def upload_file_to_s3_bucket(bucket, file_path, key_name):
	"""
	"""
	#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
	#SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
	s3_client = boto3.client('s3')
	try:
		response = s3_client.upload_file(
			Bucket=bucket,
			Filename=file_path, 
			Key=key_name
		)
		logger.info(f"Uploaded \'{key_name.split('/')[-1]}\' to \'{bucket}\' bucket.")
	except Exception as e:
		logger.error(f"Failed to upload \'{key_name.split('/')[-1]}\' to \'{bucket}\' bucket: {e}")


def delete_local_files(*file_paths):
	"""
	"""
	for file_path in file_paths:
		try:
			os.remove(file_path)
		except Exception as e:
			logger.error(f"Could not delete files in {file_path}. Error: {e}")


def update_dynamodb(job_id, results_key, log_key, complete_time):
    """
    """
    #Update status if job is pending
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
    try:
        response_running = table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET job_status = :status",
            ConditionExpression="job_status = :pending",
            ExpressionAttributeValues={
                ":status": "RUNNING", 
                ":pending": "PENDING"
            },
        )
        logger.info("Updated job status to RUNNING.")
    except boto3.dynamodb.conditions.ConditionalCheckFailedException:
        logger.error("Update failed: Job status is not PENDING.")
        return
    #Update info when job completed
    try:
        response_completion = table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="""
                SET s3_key_result_file = :rkey,
                    s3_key_log_file = :lkey,
                    complete_time = :ctime,
                    job_status = :status
            """,
            ExpressionAttributeValues={
                ":rkey": results_key,
                ":lkey": log_key,
                ":ctime": complete_time,
                ":status": "COMPLETED",
            },
        )
        logger.info("Updated job status to COMPLETED.")
    except Exception as e:
        logger.error(f"Failed to update DynamoDB on completion: {e}")


def notify_glacier_of_free_job_completion(job_id):
    """
    Notify Glacier of the completion of a job for a free user.
    """
    try:
        #Extract job's relevant parameters
        #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
        response = table.get_item(Key={'job_id': job_id})
        data = {
            'job_id': job_id,
            's3_key': response['Item'].get('s3_key'),
            'result_key': response['Item'].get('s3_key_result_file').split('/')[-1],
            'user_role': response['Item'].get('user_role'),
            'complete_time': int(response['Item'].get('complete_time'))
        }
        #Publish message to Glacier
        #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
        if data['user_role'] == "free_user":
            try:
                sns.publish(
                    TopicArn=config['aws']['AWS_SNS_GLACIER_ARCHIVE_TOPIC'],
                    Message=json.dumps(data)
                )
                logger.info("Successfully notified Glacier of job completion.")
            except ClientError as e:
                logger.error(f"Failed to notify Glacier, AWS ClientError: {e}")
            except Exception as e:
                logger.error(f"Failed to notify Glacier of job completion: {e}")
    except ClientError as e:
        logger.error(f"Error fetching item from DynamoDB: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


################################################################################
# MAIN
################################################################################

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
            #Load inputs
            filename = sys.argv[1]
            filename_dir = filename[:filename.rfind('/')]
            #Find files to upload
            files_to_upload = []
            for file in os.listdir(filename_dir):
                file_path = os.path.join(filename_dir, file).strip()
                if file.endswith(".annot.vcf") or file.endswith(".count.log"):
                    files_to_upload.append(file_path)
            #Get S3 key
            job_id = filename.split('/')[2]
            response = table.get_item(Key={'job_id': job_id})
            s3_key_prefix = response['Item'].get('s3_key').rpartition('/')[0]
            #Upload and delete local files
            for file in files_to_upload:
                key = f"{s3_key_prefix}/{file.split('../jobs/')[1]}"
                upload_file_to_s3_bucket(s3_outputs_bucket, file, key)
                if file.endswith(".annot.vcf"):
                    result_key = f"{response['Item'].get('s3_key').split('~')[0]}/{file.split('/')[-1]}"
                elif file.endswith(".count.log"):
                    log_key = f"{response['Item'].get('s3_key').split('~')[0]}/{file.split('/')[-1]}"
                delete_local_files(file)
            delete_local_files(filename)
            os.rmdir(filename_dir)
            logger.info(f"All local files deleted.")
            #Update job info in DB
            job_id = filename_dir.split('/')[-1]
            complete_time = int(time.time())
            update_dynamodb(job_id, result_key, log_key, complete_time)
            #Send email to user (PENDING)
            try:
                data = {
                    "email": response['Item'].get('user_email'),
                    "subject": f"Annotation job complete!",
                    "body": f"Job for {response['Item'].get('input_file_name')} is complete. Please log into your session and see results."
                }
                sns.publish(
                    TopicArn=config['aws']['AWS_SNS_JOB_RESULTS_TOPIC'],
                    Message=json.dumps(data)
                )
                logger.info("Job completion notification published.")
            except Exception as e:
                logger.error(f"Failed to email user regarding job completion: {e}")
            #Notify glacier queue of job completion
            try:
                notify_glacier_of_free_job_completion(job_id)
            except Exception as e:
                logger.error(f"Failed to notify glacier queue of job completion: {e}")
    else:
        logger.error("Usage: <HW_ID>_run.py <path>/<input_filename>.vcf")

### EOF
