"""
File Name: Backup.py

Description: This is a program that can be used to backup files in a directory to 
            an AWS S3 bucket. The directory to be backed up can be passed in as
            an argument to the program when executing.
            The contents will be uploaded to a bucket named ____________.
            You must have your .aws file set up with credentials to be used by
            the boto3 module for programatic AWS Access.

Assumptions: The program assumes that the embedded bucket name is a valid one. AWS
            bucket names must be unique. IF the program crashes because of this, you 
            must change the bucket name variable in main().
"""
import sys
import boto3
import os.path
import time
import argparse
import datetime
from botocore.exceptions import ClientError
import random

parser = argparse.ArgumentParser(
    description="Backup a local directory to AWS S3 bucket"
)
parser.add_argument(
    "-d",
    "--directory",
    type=str,
    metavar="",
    help="Full path to local directory.",
)
parser.add_argument(
    "-b",
    "--bucketName",
    type=str,
    metavar="",
    help="Name of AWS S3 bucket. Must follow AWS bucket naming conventions.",
)
args = parser.parse_args()

"""
travel_down
This method is to be used when all files and directories inside the 
path passed in need to be backed up to bucketName. Everything will be
put into the bucketName.
---------------------------
Returns the total number of bytes uploaded
"""


def travel_down(s3, bucketName, absPath, totalBytes, uploadKey):
    # files in the directly
    files = os.listdir(absPath)
    total = totalBytes
    # directory list
    direct = []
    for f in files:
        fullPath = os.path.join(absPath, f)
        if os.path.isdir(fullPath) == False:
            total = create_and_backup(s3, bucketName, fullPath, total, uploadKey + f)
        else:
            direct.append(f)

    for i in direct:
        uploadDirKey = uploadKey + i + "/"
        fullPath = os.path.join(absPath, i)
        # create empty dir
        total = create_and_backup(s3, bucketName, fullPath, total, uploadDirKey)
        # traverse the files in this directory
        total = travel_down(s3, bucketName, fullPath, total, uploadDirKey)

    return total


# END travel_down

"""
create_bucket
This method will be the 1st method to run in this program to always ensure that
the bucket is created or exists priot to backing up files and directories
"""


def create_bucket(s3, bucketName):
    # attempt to retrieve bucketName creation date (None is doesnt exist)
    date = s3.Bucket(bucketName).creation_date
    if date is None:
        # my best attempt at retry logic for error codes 400 - 599
        retry = 0
        while True:
            try:
                print("Creating bucket with name:", bucketName)
                response = s3.create_bucket(
                    ACL="public-read",
                    Bucket=bucketName,
                    CreateBucketConfiguration={"LocationConstraint":"us-west-2"}
                )
                print("Bucket Name: {0} - Created successfully".format(bucketName))
                break
            except ClientError as error:
                code = error.response['ResponseMetadata']['HTTPStatusCode']
                if (retry > 0):
                    print("Retry number", retry, "has code =", code)
                if (((code < 499 and code >= 400) or (code < 599 and code >= 500)) and (retry < 3)):
                    print("****** Please Wait ******\n******* Retrying *******")
                    time.sleep(2)
                    retry = retry + 1
                else:
                    print(error)
                    print("--- Unable to create Bucket for backup ---\nTRY AGAIN")
                    sys.exit()
            except Exception as error:
                print(error)
                print("*TRY AGAIN*\n**Please validate the name of the bucket**")
                sys.exit()
    else:
        print(bucketName, "already exists.")
        print(
            "Date created = {0}".format(
                s3.Bucket(bucketName).creation_date.strftime("%Y %B %d %H:%M:%S")
            )
        )


# END create_bucket

"""
backup_file
Use this method when the directory already exists in bucketName.
This will only upload files from path that have been modified after
the passed in date.
---------------------------
Returns the total number of Bytes uploaded
"""


def backup_file(s3, bucketName, path, date, totalBytes, uploadKey, toCheck):
    # store directories
    direct = []
    # retrieve all files in this path
    files = os.listdir(path)
    total = totalBytes
    for f in files:
        fullPath = os.path.join(path, f)
        if os.path.isdir(fullPath) == False:
            if ((uploadKey + f) in toCheck):
                strMDate = time.ctime(os.path.getmtime(fullPath))
                dateToCheck = datetime.datetime.strptime(strMDate, "%a %b %d %H:%M:%S %Y")
                if dateToCheck > date:
                    total = create_and_backup(
                        s3, bucketName, fullPath, total, uploadKey + f
                    )
            else:
                total = create_and_backup(
                        s3, bucketName, fullPath, total, uploadKey + f
                    )
        else:
            direct.append(f)

    for i in direct:
        # update directory meta data to reflect new backup date
        update_meta(s3, bucketName, uploadKey + i + "/")
        total = backup_file(
            s3, bucketName, os.path.join(path, i), date, total, uploadKey + i + "/", toCheck
        )

    return total


# END backup_files

"""
update_meta
This method will update the Metadata 'mod' to the current date and time
for the passed in key that needs to represent a directory in bucketName.
"""


def update_meta(s3, bucketName, key):
    # key structure for this backup program to remove Drive
    date = datetime.datetime.today()
    s3.Object(bucketName, key).put(Metadata={"mod": date.strftime("%Y %B %d %H:%M:%S")})


# END update_meta

"""
exists
This method will check if the key passed in that needs to represent a directory
exists in bucketName or not.
---------------------------
Returns True and date of last backup if key exists
        False and None if not
"""


def exists(s3, bucketName, key):
    # metadata acces s3.Object(bucketName, key).metadata
    bucket = s3.Bucket(bucketName)
    objectList = list(bucket.objects.filter(Prefix=key))
    modDate = None
    if len(objectList) > 0 and objectList[0].key == key:
        mod = s3.Object(bucketName, key).metadata["mod"]
        modDate = datetime.datetime.strptime(mod, "%Y %B %d %H:%M:%S")
        return True, modDate
    else:
        return False, modDate


# END exists

"""
create_and_backup
This method is used to create a directory into an AWS s3 bucket.
ONLY use when the directory does not already exist in bucketName.
Will create an empty object that will represent the directory path on
local machine and all files inside will begin with that path.
---------------------------
Returns the total bytes of uploaded data
"""


def create_and_backup(s3, bucketName, absPath, totalBytes, uploadKey):
    retry = 0
    total = totalBytes
    # Again, my best attemp at retry logic for status codes 400 - 599
    while True:
        try:
            if os.path.isdir(absPath) == True:
                # create date to write into meta data
                date = datetime.datetime.today()
                response = s3.Object(bucketName, uploadKey).put(
                    uploadKey,
                    ACL="public-read",
                    Metadata={"mod": date.strftime("%Y %B %d %H:%M:%S")},
                )
                print(
                    "Upload - {0} - as empty object to represent local directory".format(absPath)
                )
                break
            else:
                data = open(absPath, "rb")
                response = s3.Object(bucketName, uploadKey).put(uploadKey, ACL="public-read", Body=data)
                total = totalBytes + os.path.getsize(absPath)
                print("Upload - {0} - as file".format(absPath))
                break
        except ClientError as error:
            code = response['ResponseMetadat']['HTTPStatusCode']
            if (retry > 0):
                print("Retry number", retry, "has code =", code)
            if (((code < 499 and code >= 400) or (code < 599 and code >= 500)) and (retry < 3)):
                print("****** Please Wait ******\n******* Retrying *******")
                time.sleep(2)
                retry = retry + 1
            else:
                print(error)
                break
        except Exception as error:
            print(error)

    return total


# END create_and_backup

"""
"""


def formatBytes(totBytes):
    tot = None
    if totBytes > pow(2, 20):
        tot = "Uploaded {0:.2f}MB".format(totBytes / pow(2, 20))
    elif totBytes > pow(2, 10):
        tot = "Uploaded {0:.2f}KB".format(totBytes / pow(2, 10))
    elif totBytes == 0:
        return "No Files were modified since last Backup"
    else:
        tot = "Uploaded {0}Bytes".format(totBytes)

    return tot


# END formatBytes

"""
"""


def envKey(path):
    if sys.platform.startswith("win"):
        wordList = path.split("\\")
        return wordList[-1] + "/"
    else:
        wordList = path.split("/")
        return wordList[-1] + "/"


# END envKey

'''
'''
def get_bucket_directory(args):
    if args.directory is None:
        print("No directory passed in\n*current directory:", os.getcwd())
        path = os.getcwd()
    else:
        print(args.directory)
        path = args.directory
    if args.bucketName is None:
        print("----- No bucketName passed in -----\n--will use stock bucketName below--")
        seed = boto3.client("sts").get_caller_identity().get("Account")
        random.seed(seed)
        bucketName = (
            "css436-dirbackup-"
            + str(random.randint(10, 99))
            + "-"
            + str(random.randint(10, 99))
            + "-"
            + str(random.randint(10, 99))
        )
        print(bucketName)
    else:
        print(args.bucketName)
        bucketName = args.bucketName

    return path, bucketName


def main():
    path, bucketName = get_bucket_directory(args)
    ######### UNIQUE BUCKET NAME #########
    # bucketName = "prog3-backup-test1561"
    # bucketName = "mybucket45"
    ######################################
    s3 = boto3.resource("s3")
    # path = args.directory
    baseUploadDir = envKey(path)
    totalBytes = 0

    create_bucket(s3, bucketName)
    ###### check if there is a directory with the same name ######
    exist, modDate = exists(s3, bucketName, baseUploadDir)
    if exist == False and modDate is None:
        print("Directory does not exist in :", bucketName)
        print("Backing up files/folders for :", path)
        print(
            "Directory will be named:",
            baseUploadDir,
            " in AWS s3 bucket named:",
            bucketName,
        )
        # create the directory and backup all files
        create_and_backup(s3, bucketName, path, totalBytes, baseUploadDir)
        totalBytes = travel_down(s3, bucketName, path, totalBytes, baseUploadDir)
    else:
        # read metadata to get last backup date and push new files up
        print("{0} already exists in {1}".format(baseUploadDir, bucketName))
        print("Date of Last Backup:", modDate)
        print("Uploading modified files ONLY...")
        objList = list(s3.Bucket(bucketName).objects.filter(Prefix=baseUploadDir))
        toCheck = []
        for i in objList:
            toCheck.append(i.key)
        totalBytes = backup_file(
            s3, bucketName, path, modDate, totalBytes, baseUploadDir, toCheck
        )
        update_meta(s3, bucketName, baseUploadDir)

    tot = formatBytes(totalBytes)
    print(tot)
    print("##  Thank you for using DirBackup  ##\n---------- Goodbye World! ----------")


# END main

if __name__ == "__main__":
    main()

