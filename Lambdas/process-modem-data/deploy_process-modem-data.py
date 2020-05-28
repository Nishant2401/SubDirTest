#!/usr/bin/env python

import os
import shutil

def deploy():
    # Change bucket to account specific lambda zips bucket
    bkt = 'gtt-lambda-zips-us-west-2'

    home = os.getcwd()
    # put required libraries in package directory
    cmd = 'pip install --target ./package requests'
    os.system(cmd)
    print(cmd)

    os.chdir('package')
    print('cd package')

    # zip package directory
    cmd = f'zip -r9 {home}/process-modem-data.zip .'
    os.system(cmd)
    print(cmd)

    os.chdir(home)
    print('cd ..')

    # add lambda to zip 
    cmd = 'zip -g process-modem-data.zip process-modem-data.py'
    os.system(cmd)
    print(cmd)

    # upload to s3 
    cmd = f'aws s3 cp process-modem-data.zip s3://{bkt}'
    os.system(cmd)
    print(cmd)

    # remove package directory
    cmd = 'rm -rf package'
    os.system(cmd)
    print(cmd)

    # remove zip file
    cmd = 'rm process-modem-data.zip'
    os.system(cmd)
    print(cmd)


def main():
    deploy()

if __name__ == "__main__":
    main()