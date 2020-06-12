###################################
# run-analysis.py
###################################
"""
Author: Matt Balshaw
Start Date: 10/06/2020

Runs the analysis on a set of Facebook Messenger data using a specified config
"""

import json

from scripts.unzipper import ZipFuncs
from scripts.processor import ProcessorFuncs

from scripts.sendreceive import SendReceiveData
from scripts.messagesovertime import MessagesOverTime


if __name__ == "__main__":

    print("====================================")
    print("⛏ Starting the Messenger Analysis")
    print("====================================")

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

    SendReceiveData().output_send_receive(config)

    config['debug'] = True

    messagesOverTimeConfig = config['messagesOverTime']

    MessagesOverTime().make_graphs(config, messagesOverTimeConfig)

    print("====================================")
    print("✓ Messenger Analysis Complete")
    print("====================================")
