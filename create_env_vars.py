import os

os.system('setx AWS_RDS_ENDPOINT kakepa-final-mysql-db-1.c04vdpgrzemq.us-east-1.rds.amazonaws.com')
os.system('setx AWS_RDS_DATABASE DBTest1')
os.system('setx AWS_RDS_USERNAME admin')
os.system('setx AWS_RDS_PASSWORD BlueDolphin11!')

print("Environment variables set successfully.")
