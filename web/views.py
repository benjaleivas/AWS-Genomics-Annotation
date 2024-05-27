######################################################################################################
# SETUP
######################################################################################################

#Dependencies
import uuid
import time
import json
import boto3
from gas import app, db
from botocore.client import Config
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
from auth import get_profile, update_profile
from decorators import authenticated, is_premium
from flask import (abort, flash, redirect, render_template, request, session, url_for)
from botocore.exceptions import (ClientError, NoCredentialsError, PartialCredentialsError)

######################################################################################################
# ENDPOINTS
######################################################################################################

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  #Create a session client to the S3 service
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html 
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  #Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  #Create the redirect URL
  redirect_url = str(request.url) + '/job'

  #Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  #Generate the presigned POST call
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_post.html
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  #Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  #Resources
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  
  #Profile
  profile = get_profile(identity_id=session.get('primary_identity'))

  #Extract relevant info from redirect URL
  try:
    user_id = session['primary_identity']
    user_name = profile.name
    user_email = profile.email
    user_role = profile.role
    s3_key = str(request.args.get('key'))
    job_id = s3_key.split('/')[2].split('~')[0]
    bucket_name = str(request.args.get('bucket'))
    input_file_name = s3_key.split('~')[-1].strip()
  except Exception as e:
    app.logger.error(f"Failed to retrieve arguments from URL: {e}")

  #Persist job to database
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/put_item.html
  data = {
    "user_id": user_id,
    "user_name": user_name,
    "user_email": user_email,
    "user_role": user_role,
    "s3_key": s3_key,
    "job_id": job_id,
    "input_file_name": input_file_name,
    "bucket_name": bucket_name,
    "submit_time": int(time.time()),
    "job_status": 'PENDING'
  }
  try:
    table.put_item(Item=data)
  except Exception as e:
    app.logger.error(f"Failed to add new item to DynamoDB table: {e}")

  #Send message to request queue
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
  try:
    sns.publish(
      TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
      Message=json.dumps(data)
      )
    app.logger.info("Job request message published")
  except Exception as e:
    app.logger.error(f"""
    Failed to publish job request notification message via SNS: {e}
    """)

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  #Resources
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # NOTE: Made 'user_id' a Global Secondary Index in DynamoDB console

  #Retrieve annotations
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
  try:
    response = table.query(
        IndexName='UserIdIndex',
        KeyConditionExpression=Key('user_id').eq(session['primary_identity']),
        ProjectionExpression="job_id, submit_time, input_file_name, job_status"
    )
    annotations = response.get('Items')
  except Exception as e:
    app.logger.error(f"Failed to retrieve user's annotations: {e}")

  #Make submit time readable
  for annotation in annotations:
    annotation['submit_time'] = datetime.fromtimestamp(int(annotation['submit_time'])).strftime('%Y-%m-%d %H:%M')

  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):

    #Resources
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html 
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version='s3v4'))

    #Retrieve job details
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
    try:
        response = table.query(
            KeyConditionExpression=Key('job_id').eq(id),
            ProjectionExpression="job_id, job_status, submit_time, input_file_name, complete_time, s3_key_result_file, s3_key_log_file, user_id, s3_key"
        )    
        annotation = response.get('Items')[0]
    except Exception as e:
        app.logger.error(f"Failed to retrieve job annotation: {e}")
        return render_template("error.html", message="Failed to retrieve job annotation."), 500

    #Validate user
    if annotation["user_id"] != session["primary_identity"]:
        return render_template("error.html", message="Not authorized to view this job"), 403

    #Clean details
    annotation["submit_time"] = datetime.fromtimestamp(int(annotation["submit_time"])).strftime("%Y-%m-%d %H:%M")
    if "complete_time" in annotation:
        complete_time_str = str(annotation["complete_time"])
        complete_time = datetime.fromtimestamp(float(complete_time_str))
        annotation["complete_time"] = complete_time.strftime("%Y-%m-%d %H:%M")

    #Job results availability
    free_access_expired = False
    restore_message = False
    if annotation["job_status"] == 'COMPLETED':
        complete_time = datetime.strptime(annotation["complete_time"], "%Y-%m-%d %H:%M")
        current_time = datetime.utcnow()
        free_user_data_retention = timedelta(seconds=app.config['FREE_USER_DATA_RETENTION'])

        #If user is 'free' and more than the retention period has passed since completion
        if (current_time - complete_time >= free_user_data_retention) and session.get('role') == 'free_user':
            free_access_expired = True
        #If user switched to 'premium' recently, but file not yet retrieved
        elif "s3_key_result_file" not in annotation or not annotation["s3_key_result_file"]:
            restore_message = True
            annotation['restore_message'] = """
                This file is not currently available. 
                If you are a free user, the download window has closed, and 
                your results have been archived. If you want to access them, 
                please suscribe as a premium member and check back later. 
            """
        #If file is still available in bucket, being free user or not
        else:
            #Build pre-signed download url
            #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
            try:
              result_file_name = annotation['s3_key_result_file'].split('/')[-1]
              key = annotation['s3_key'].split('~')[0]+'/'+result_file_name
              print("key:", key)
              app.logger.info(f"key: {key}")
              presigned_url = s3.generate_presigned_url(
                  ClientMethod='get_object', 
                  Params={
                      'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                      'Key': key
                  }, 
                  ExpiresIn=3600)
              annotation['result_file_url'] = presigned_url
            except ClientError as e:
                app.logger.error(f"Could not generate presigned URL for results file: {e}")

    return render_template(
        'annotation_details.html', 
        annotation=annotation,
        free_access_expired=free_access_expired,
        restore_message=restore_message
    )


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):

  #Resources
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html 
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version='s3v4'))

  #Fetch log file
  #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
  try:
    response = table.query(
      KeyConditionExpression=Key('job_id').eq(id),
      ProjectionExpression="s3_key, input_file_name"
    )
    annotation = response.get('Items')[0]
    log_file_name = f"{annotation['input_file_name']}.count.log"
    key = annotation['s3_key'].split('~')[0]+'/'+log_file_name
    #Get object
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    obj = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=key)
    log_file_contents = obj['Body'].read().decode('utf-8')
  except ClientError as e:
    app.logger.error(f"ClientError in fetching log file: {e}")
    return render_template("error.html", message=f"Could not fetch log file: {e}"), 500
  except NoCredentialsError or PartialCredentialsError as e:
    app.logger.error(f"Credential error in fetching log file: {e}")
    return render_template("error.html", message="Credential issues while accessing S3."), 500
  except Exception as e:
    app.logger.error(f"Unexpected error: {e}")
    return render_template("error.html", message=f"Unexpected error occurred: {e}"), 500

  return render_template("view_log.html", job_id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    #Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    #Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    #Update role in the session
    session['role'] = "premium_user"

    #Request restoration of the user's data from Glacier
    #Add code here to initiate restoration of archived user data
    #Make sure you handle files not yet archived!
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    #SOURCE: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html
    sqs = boto3.client(
      'sqs',
      region_name=app.config['AWS_REGION_NAME'], 
      config=Config(signature_version='s3v4')
      )
    sqs.send_message( 
      QueueUrl=app.config['AWS_SQS_GLACIER_RESTORE_QUEUE_URL'], 
      MessageBody=str({'user_id': session['primary_identity']})
    )

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
