###################################
# processor.py
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


class ProcessorFuncs():

    def run_processing(self, config):
        processedLoc = "processed-data/" + config['zipFile'] + "/"
        rawLoc = "raw-data/" + config['zipFile']

        # Check if file exists
        doesExist, errorMessage = self.check_processed(processedLoc)
        if not doesExist or config['debug']:
            print(errorMessage)
            self.process_data(rawLoc, processedLoc, config['myName'])
        else:
            print("♻ Using existing processed data")

        doesExist, errorMessage = self.check_processed(processedLoc)
        if not doesExist:
            print(errorMessage)
            print("☠ Critical error, exiting...")
            sys.exit()
        else:
            print("✓ Processing success")

    def check_processed(self, loc):
        # Ensure that the directory exists and is correct

        contactLoc = loc + "contactData.csv"
        messagesLoc = loc + "allMessageData.csv"

        if not os.path.exists(contactLoc):
            return False, "No contact file found"

        if not os.path.exists(messagesLoc):
            return False, "No Message file found"

        return True, "Found processed data"

    def process_data(self, rawLoc, processedLoc, myName):
        # Create a contact dataframe and message dataframe

        print("Gathering message files...")

        messageFiles = []

        inboxLoc = rawLoc + "/messages/inbox/"

        for root, dirs, files in tqdm(os.walk(inboxLoc, topdown=True)):
            for name in files:
                if name[-5:] == ".json":
                    # Also test if has more than 4 slashes...
                    path = os.path.join(root, name)
                    slashes = path.count('/')
                    if slashes < 6:
                        # Matches the last folder in the root path
                        # This gets us a contact ID
                        p = re.compile('\\w[a-zA-Z0-9_\\-]+$')
                        cID = p.findall(root)[0]
                        msgFile = {
                            "path": path,
                            "contactID": cID
                        }
                        messageFiles.append(msgFile)

        contactDF = pd.DataFrame()
        allMessagesDF = pd.DataFrame()

        print("Filtering messages and creating dataframes...")

        # Now for each file, load file
        # Get file contact name
        # put all messages in messageList
        for messageFile in tqdm(messageFiles):
            with open(messageFile['path'], "r", encoding="utf-8") as f:
                messages = json.load(f)
                # Ignore group chats
                if len(messages['participants']) > 2:
                    continue

                contactID = messageFile['contactID']

                # Ignore facebook user and myself
                cName = "Unknown"
                for participant in messages['participants']:
                    if participant['name'] == "Facebook User":
                        continue
                    if participant['name'] != myName:
                        # Use ftfy to fix terrible facebook unicode
                        cName = ftfy.fix_text(participant['name'])

                if cName == "Unknown":
                    continue

                # Create contact
                newContact = {
                    "contactID": [contactID],
                    "name": [cName],
                    "messagepart": [len(messages['messages'])],
                }
                newDF = pd.DataFrame(newContact)
                contactDF = contactDF.append(newDF)

                receivedList = []
                timestampsList = []
                contentList = []
                typeList = []
                pathList = []
                oneList = []
                nameList = []

                # Create messages
                for message in messages['messages']:

                    # Ignore gifs and links, only want text
                    if 'content' not in message:
                        continue
                    timestampsList.append(message['timestamp_ms'])
                    oneList.append(1)
                    # Use ftfy to fix terrible facebook unicode
                    contentList.append(ftfy.fix_text(message['content']))
                    typeList.append(message['type'])
                    pathList.append(contactID)
                    nameList.append(cName)
                    if message['sender_name'] == myName:
                        receivedList.append(0)
                    else:
                        receivedList.append(1)

                newMessages = {
                    "contactID": pathList,
                    "name": nameList,
                    "received": receivedList,
                    "timestamp_ms": timestampsList,
                    "content": contentList,
                    "type": typeList,
                    "msg": oneList,
                }

                newMessageDF = pd.DataFrame.from_dict(newMessages)
                allMessagesDF = allMessagesDF.append(newMessageDF)

        # now concatenate all the contacts
        contactDF['messages'] = contactDF.groupby(
            ['contactID'])['messagepart'].transform('sum')
        contactDF = contactDF.drop_duplicates(subset=['contactID'])
        contactDF = contactDF.drop(columns=["messagepart"])

        # Now sort the dataframes
        contactDF = contactDF.sort_values(by=['messages'], ascending=False)
        allMessagesDF = allMessagesDF.sort_values(
            by=['timestamp_ms'], ascending=True)

        # Determine the filenames
        contactLoc = processedLoc + 'contactData.csv'
        messageLoc = processedLoc + 'allMessageData.csv'

        # Ensure path exists
        Path(processedLoc).mkdir(parents=True, exist_ok=True)

        print("Processed %s contacts and %s messages" %
              (len(contactDF), len(allMessagesDF)))

        # Print the files
        contactDF.to_csv(contactLoc)
        allMessagesDF.to_csv(messageLoc)
