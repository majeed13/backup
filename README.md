# backup
console app to backup a local directory to AWS S3 bucket


IMPORTANT INFORMATION
Please make sure that you have .aws folder with config and credentials files set up correctly.
Currently if config file has region specified to: us-east-1, the location constraint on the bucket creation will not work.
IF THAT IS AN ERROR THAT OCCURS WHEN YOU RUN THE PROGRAM – please change the region in the config file to: us-west-2.
The credentials file should contain your AWS_KEY and AWS_SECRET_KEY.

Backup is designed to be run from the command terminal with Python 3.7.4 and assumes that the machine running it also has the boto3 library installed.

HOW TO RUN
If .aws folder on your machine is formatted correctly, you should be able to navigate to the directory where Backup.py resides 
in and run the following command to run the program:

python Backup.py [-h] [-d] [-b] OR python3 Backup.py [-h] [-d] [-b] (depending on what the path variable is for your machines python 3)

Use -h flag for help, -d flag to specify a directory to backup, -b flag to specify a bucket that you would like the backup to go into.
