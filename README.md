# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Setup
1. Run `python launch_web_instance.py` to create an EC2 instance you can SSH later.
2. Manually SSH into instance by running ssh -A -i ~/.ssh/bleiva.pem -o StrictHostKeyChecking=no ec2-user@<PUBLIC-IPv4-DNS>.
3. Activate virtual environment `source mpcs-cc/bin/activate`
4. Go to `/home/ec2-user/mpcs-cc/gas/web` and create `.env` file, then run `source .env`
5. Run `python manage.py db init`, then `python manage.py db migrate`, and finally `python manage.py db upgrade`.
6. Go to `/home/ec2-user/mpcs-cc/gas/web/config.py` and replace '<CNETID>' with 'bleiva'.
7. Run `chmod 755 run_gas.sh` to make file executable, then run `./run_gas.sh console`, and then go to https://bleiva.mpcs-cc.com:4433, the webpage should be up.

