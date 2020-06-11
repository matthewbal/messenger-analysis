###################################
# sendreceive.py
###################################
"""
Author: Matt Balshaw
Start Date: 10/06/2020

Processes the raw facebook data into usable csv's
"""

import pandas as pd
import os
import json
import sys
from tqdm import tqdm
from pathlib import Path
import re
import ftfy


class SendReceiveData():

    # Given the allMessages.csv, outputs a sorted csv

    def output_send_receive(self, config):

        # Checks that we have the allMessagesData

        processedLoc = "processed-data/" + config['zipFile'] + "/"
        outpotLoc = "output-data/" + config['zipFile'] + "/"

        Path(outpotLoc).mkdir(parents=True, exist_ok=True)

        allMsgLoc = processedLoc + "allMessageData.csv"
        valueFileLoc = outpotLoc + "sendreceivecontacts.csv"

        if not os.path.exists(allMsgLoc):
            print("Can't find the allMessageData.csv")
            print("☠ Critical error, exiting...")
            sys.exit()

        if os.path.exists(valueFileLoc) and not config['debug']:
            print("♻ Found existing sendreceivecontacts.csv")
            return True

        print("Calculating our send/receive data")

        with tqdm(total=60) as pbar:

            allMessages = pd.read_csv(allMsgLoc)

            pbar.update(10)

            allMessages['len'] = allMessages.apply(
                lambda row: len(row.content), axis=1)

            pbar.update(10)

            allMessages['partLen'] = allMessages.groupby(['name', 'received'])[
                'len'].transform('sum')
            allMessages['totalLen'] = allMessages.groupby(
                ['name'])['len'].transform('sum')

            allMessages = allMessages.drop_duplicates(
                subset=['name', 'received'])

            pbar.update(10)

            allMessages['pct'] = allMessages.apply(
                lambda row: (row.partLen / row.totalLen), axis=1)

            pbar.update(10)

            allMessages = allMessages[
                ['name', 'received', 'totalLen', 'partLen', 'pct']]

            allMessages = allMessages[allMessages['received'] == 1]
            allMessages = allMessages[allMessages['totalLen'] > 250]

            allMessages = allMessages.sort_values(by=['pct'], ascending=False)

            pbar.update(10)

            allMessages.to_csv(valueFileLoc)

            pbar.update(10)

        print("✓ Send/receive data calculated")
