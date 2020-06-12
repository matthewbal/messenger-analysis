###################################
# run-analysis.py
###################################
"""
Author: Matt Balshaw
Start Date: 10/06/2020

Runs the analysis on a set of Facebook Messenger data using a specified config
"""

import json
import shutil
import os

from scripts.unzipper import ZipFuncs
from scripts.processor import ProcessorFuncs

from scripts.topcontactdetails import TopContactDetails
from scripts.messagesovertime import MessagesOverTime


if __name__ == "__main__":

    print("====================================")
    print("⛏ Starting the Messenger Analysis")
    print("====================================")

    # Do example config thing...

    if not os.path.exists('config.json'):
        print("Loading Example Config into Config")
        shutil.copy('exampleconfig.json', 'config.json')

    with open('config.json', 'r') as f:
        config = json.load(f)

    if config['debug']:
        print("⚠ Debugging... will force re-run all processing")

    print("##############################")
    print("# Unzipping Data")
    print("##############################")

    ZipFuncs().run_unzip_process(config)

    print("##############################")
    print("# Processing Data")
    print("##############################")

    ProcessorFuncs().run_processing(config)

    print("##############################")
    print("# Generating Graphs")
    print("##############################")

    TopContactDetails().generate_all_graphs(config)

    messagesOverTimeConfig = config['messagesOverTime']

    MessagesOverTime().make_graphs(config, messagesOverTimeConfig)

    print("====================================")
    print("✓ Messenger Analysis Complete")
    print("====================================")
