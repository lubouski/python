#!/usr/bin/python

import sys
import urllib
import urllib2
import os
import threading
import boto3
import botocore

links = sys.argv[1:]
cwd = os.getcwd()
s3 = boto3.resource('s3')

def worker(link):
    """thread worker function"""
    schema = link.split('//',1)[0][:-1]
    filename = link.rsplit('/',1)[-1]
    bucket = link.split('//')[1].split('/')[0]
    req = urllib2.Request(link)
    if schema == 'http' or schema == 'https':
        try:
            resp = urllib2.urlopen(req)
            print("Beginning file download from {0}".format(link))
            urllib.urlretrieve(link, cwd + '/' + filename)
            print("File {0} dowmloaded".format(filename))
        except urllib2.HTTPError as e:
            print("Error to download {0}".format(link))
    elif schema == 's3':
        try:
            s3.Bucket(bucket).download_file(link, cwd + '/' + filename)
            print("The object {0} downloaded".format(filename))
        except botocore.exceptions.ClientError as e:
            print("The object {0} does not exist".format(filename))
    return

def concurency_download():
    threads = []
    for link in links:
        t = threading.Thread(target=worker, args=(link,))
        threads.append(t)
        t.start()

concurency_download()
