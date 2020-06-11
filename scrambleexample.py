###################################
# scrambleexample.py
###################################
"""
Author: Matt Balshaw
Start Date: 11/06/2020

Scrambles an example set of messages to anonymize them
"""

import names
import os
import json
import re
from essential_generators import DocumentGenerator
from tqdm import tqdm
import random
import shutil

if __name__ == "__main__":

    inboxLoc = "raw-data/facebook-example/messages/inbox/"

    myname = "MyFirst MyLast"

    messageFiles = []
    contactDirs = []

    # Walk through each folder in the example directory
    for root, dirs, files in os.walk(inboxLoc, topdown=True):
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
                    if root not in contactDirs:
                        contactDirs.append(root)

    print("Filtering messages and creating dataframes...")

    existingMap = {}

    # We first want to scramble each message file
    for messageFile in messageFiles:
        print("Loading next file...")
        with open(messageFile['path'], "r", encoding="utf-8") as f:
            messages = json.load(f)

        contact1 = messages['participants'][1]['name']
        newcontact1 = myname

        # We need to ensure that if multiple message files, they all get same
        # name.
        contact2 = messages['participants'][0]['name']
        if contact2 not in existingMap:
            newcontact2 = names.get_full_name()
            existingMap[contact2] = newcontact2
        else:
            newcontact2 = existingMap[contact2]

        messages['participants'][0]['name'] = newcontact1
        messages['participants'][1]['name'] = newcontact2

        messages['title'] = newcontact2
        messages['thread_path'] = "inbox/redacted"

        gen = DocumentGenerator()
        print("Scrambling messages in %s" % messageFile['path'])

        newMessages = []
        for message in tqdm(messages['messages']):

            # Safely generate a new message, based on the old one.
            newMessage = {}

            if message['sender_name'] == contact1:
                newMessage['sender_name'] = newcontact1
            elif message['sender_name'] == contact2:
                newMessage['sender_name'] = newcontact2

            newMessage['timestamp_ms'] = message['timestamp_ms'] + \
                random.randrange(-300000, 300000)

            randSentence = gen.sentence()
            # Regular csv's don't like commas
            randSentence = randSentence.replace(",", "")
            randSentence = randSentence.replace(";", "")

            newMessage['content'] = randSentence
            newMessage['type'] = "Generic"

            newMessages.append(newMessage)

        messages['messages'] = newMessages

        print("Saving changes...")
        with open(messageFile['path'], "w", encoding="utf-8") as f:
            f.write(json.dumps(messages))

    # Then we want to scramble the contact ID's

    print("Changing folder names...")
    for contactDir in tqdm(contactDirs):

        randID = ""

        # Add a random end string to the contact
        for x in range(0, 10):
            chars = "abcdefg12345"
            randID += (chars[random.randrange(0, len(chars))])

        fPath = contactDir + "/message_1.json"

        with open(fPath, "r", encoding="utf-8") as f:
            messages = json.load(f)
            title = messages['title']
            title = title.replace(" ", "")

        newContactDir = title + "_" + randID
        newPath = inboxLoc + newContactDir

        # Move the file to new path
        shutil.move(contactDir, newPath)

    print("✓ Scrambling complete")
