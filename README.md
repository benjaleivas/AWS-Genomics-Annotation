# Genomics Annotation Service

Directory contents are as follows:

- `/ann` - Annotator files
- `/aws` - AWS user data files
- `/web` - Web application files
- `/util` - Utility scripts for notifications, archival, and restoration

## Architecture
<img width="612" alt="aws_architecture" src="https://github.com/MPCS-51083-Cloud-Computing/final-project-benjaleivas/assets/96876463/666d3a0d-4fe8-4e87-bfd6-0c137658ac2c">

## Setup

1. Run `python launch_instance.py` in your local machine to create an EC2 instance you can SSH later. You can add the suffix `web`, `ann`, or `util` to the instance tag once it starts for use clarity.
2. Add <PUBLIC-IPv4-DNS> from AWS Instances console and your <CNET-ID> to your remote SSH config file, like this:

```
Host <PREFERED-HOST-DENOMINATION>
    HostName <PUBLIC-IPv4-DNS>
    IdentityFile `~/.ssh/<CNET-ID>.pem`
    User ec2-user
    ForwardAgent yes
```

Then, SSH into instance.

3. Activate virtual environment `source mpcs-cc/bin/activate`.
4. Go into the `mpcs-cc` directory and clone the repository for the project, as: `git clone https://github.com/MPCS-51083-Cloud-Computing/final-project-benjaleivas gas`. This will create a folder `gas` in your instance with all the necessary code.
5. If instance handles the web app:
   - Go to `/home/ec2-user/mpcs-cc/gas/web` and create `.env` file according to instructions in `final_project.pdf`, then run `source .env`.
   - Run `python manage.py db init`, then `python manage.py db migrate`, and finally `python manage.py db upgrade` (only once!).
   - Go to `/home/ec2-user/mpcs-cc/gas/web/config.py` and replace '<CNETID>' with your corresponding ID.
   - Download deployment certification and keys using `aws s3 cp s3://mpcs-cc-resources/ssl/privkey.pem /home/ec2-user/mpcs-cc` and `aws s3 cp s3://mpcs-cc-resources/ssl/fullchain.pem /home/ec2-user/mpcs-cc`, and modify the `.env` file to update the `SSL_CERT_PATH` and `SSL_KEY_PATH` to these new files.
   - Run `chmod 755 run_gas.sh` to make file executable, then run `./run_gas.sh console`, and then go to https://bleiva.mpcs-cc.com:4433, the webpage should be up.

## Troubleshooting

If you run into trouble with the instances, particularly with the web app, and need to reboot them, please follow this process. In your local machine:

1. Run `python /Users/bleiva/Desktop/CAPP/quarters/2023-2024/3_spring/cc/mpcsdomain delete --subdomain bleiva --ip <PUBLIC IPv4 ADDRESS>` with the public IP address of the instance that (currently) handles the web app.
2. Terminate the web app instance in the AWS Instances console.
3. Run `python launch_instance.py` and tag it as `<CNETID>-gas-web`.
4. Once the instance is running, run `python /Users/bleiva/Desktop/CAPP/quarters/2023-2024/3_spring/cc/mpcsdomain create --subdomain bleiva --ip <PUBLIC IPv4 ADDRESS>` in your local machine with the public IP adress of the new instance.

4. Then, we initate an `archive-retrieval` job to bring back the result files from the user's vault back to the S3 bucket. This job is also suscribed to the thaw process' topic ARN, so that we can know when the retrieval is done and the output is ready for download.
5. After initiating all these processes, we delete the message that was received by the **GLACIER_RESTORE** queue.
6. On the other hand, `thaw.py` has another queue **GLACER_THAW** listening to the messages described in step 4.
7. Once this message is received, the output is retrieved, the `s3_key_result_file` is re-generated, and the previously vaulted result files are saved back into the S3 bucket.
