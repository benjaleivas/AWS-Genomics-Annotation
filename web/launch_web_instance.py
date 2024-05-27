###############################################################################
# SETUP
###############################################################################

#Dependencies
import os
import boto3
import subprocess
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

#Parameters
CNET_ID = "bleiva"
INSTANCE_TYPE = "t2.nano"
TAG = f"{CNET_ID}-gas-web"
AMI = "ami-01648adadb9e40711"
DOM_DIR = "/Users/bleiva/Desktop/CAPP/quarters/2023-2024/3_spring/cc/mpcsdomain"

#Environment variables
GAS_HOST_PORT = "4433"
GUNICORN_WORKERS = "2"
GAS_APP_HOST = "0.0.0.0"
GAS_LOG_FILE_NAME = "gas.log"
GAS_HOST_IP = "bleiva.mpcs-cc.com"
GAS_SETTINGS = "config.ProductionConfig"
ACCOUNTS_DATABASE_TABLE = "bleiva_accounts"
SSL_KEY_PATH = "/etc/ssl/certs/mpcs-cc.com.key"
SSL_CERT_PATH = "/etc/ssl/certs/mpcs-cc.com.crt"
GAS_WEB_APP_HOME = "/home/ec2-user/mpcs-cc/gas/web"

#AWS resource
config = Config(region_name="us-east-1")
ec2 = boto3.resource("ec2", config=config)

###############################################################################
# CREATE INSTANCE
###############################################################################

try:
    print(f"Creating EC2 instance of type {INSTANCE_TYPE}...")
    instance = ec2.create_instances(
        MinCount=1,
        MaxCount=1,
        ImageId=AMI,
        KeyName=CNET_ID,
        InstanceType=INSTANCE_TYPE,
        SecurityGroups=["mpcs-cc"],
        IamInstanceProfile={"Name": f"instance_profile_{CNET_ID}"},
        TagSpecifications=[
            {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": TAG}]}
        ],
    )[0]
    print("Waiting for instance to start...")
    instance.wait_until_running()
    instance.load()
except ClientError as e:
    print(f"Failed to create EC2 instance: {e.response['Error']['Message']}")
except BotoCoreError as e:
    print(f"Boto3 error: {str(e)}")
except Exception as e:
    print(f"Error occurred: {str(e)}")

###############################################################################
# CREATE SUBDOMAIN
###############################################################################

try:
    subdom_cmd = f"python {DOM_DIR} update --subdomain bleiva-gas-web --ip {instance.public_ip_address}"
    subprocess.run(subdom_cmd, shell=True, check=True)
except OSError as e:
    print(f"Failed to create subdomain: {str(e)}")

###############################################################################
# PREPARE INSTANCE
###############################################################################

env_vars = f"""
export GAS_HOST_PORT={GAS_HOST_PORT}
export GUNICORN_WORKERS={GUNICORN_WORKERS}
export GAS_APP_HOST={GAS_APP_HOST}
export GAS_LOG_FILE_NAME={GAS_LOG_FILE_NAME}
export GAS_HOST_IP={GAS_HOST_IP}
export GAS_SETTINGS={GAS_SETTINGS}
export ACCOUNTS_DATABASE_TABLE={ACCOUNTS_DATABASE_TABLE}
export SSL_KEY_PATH={SSL_KEY_PATH}
export SSL_CERT_PATH={SSL_CERT_PATH}
export GAS_WEB_APP_HOME={GAS_WEB_APP_HOME}
"""

ssh_cmd = f"ssh -A -i ~/.ssh/bleiva.pem -o StrictHostKeyChecking=no ec2-user@{instance.public_dns_name}"

setup_cmds = [
    "cd mpcs-cc",
    "source bin/"
    "git clone git@github.com:MPCS-51083-Cloud-Computing/final-project-template.git gas",
    # "cd gas/web",
    # f'echo "{env_vars}" > /home/ec2-user/mpcs-cc/gas/web/.env',
    # "source .env",
    # "python manage.py db init",
    # "python manage.py db migrate",
    # "python manage.py db upgrade"
]

try:
    print("Setting up instance...")
    combined_cmds = " && ".join(setup_cmds)
    ssh_full_cmd = f'{ssh_cmd} "{combined_cmds}"'
    subprocess.run(ssh_full_cmd, shell=True, check=True)
    print("Setup completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during instance setup: {e}")
except Exception as e:
    print(f"Unexpected error during instance setup: {str(e)}")