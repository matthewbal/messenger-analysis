###################################
# unzipper.py
###################################
"""
Author: Matt Balshaw
Start Date: 10/06/2020

Unzips the facebook messenger download file
"""

from zipfile import ZipFile
from tqdm import tqdm
import os
import sys


class ZipFuncs():

    def run_unzip_process(self, config):
        loc = "facebook-zips/" + config['zipFile'] + ".zip"
        dest = "raw-data/" + config['zipFile']

        # Check if file exists
        doesExist, errorMessage = self.check_exists(dest)
        if not doesExist or config['debug']:
            print(errorMessage)
            self.unzip_file(loc, dest)
        else:
            print("♻ Using existing raw data")

        doesExist, errorMessage = self.check_exists(dest)

        if not doesExist:
            print(errorMessage)
            print("☠ Critical error, exiting...")
            sys.exit()
        else:
            print("✓ Unzip success")

    def check_exists(self, loc):
        # Ensure that the directory exists and is correct
        inboxLoc = loc + "/messages/inbox"
        if not os.path.exists(loc):
            return False, "No extract found"

        if not os.path.exists(inboxLoc):
            return False, "Invalid zip file used, ensure the zip used was downloaded from Facebook"

        return True, "Found raw data"

    def unzip_file(self, loc, dest):
        print("Unzipping %s to %s" % (loc, dest))

        with ZipFile(file=loc) as zip_file:

            # Loop over each file
            for file in tqdm(iterable=zip_file.namelist(), total=len(zip_file.namelist())):
                # Extract each file
                zip_file.extract(member=file, path=dest)
