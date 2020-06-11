###################################
# run-analysis.py
###################################
"""
Author: Matt Balshaw
Start Date: 10/06/2020

Runner for the project
"""

from scripts.unzipper import ZipFuncs
from scripts.processor import ProcessorFuncs

from scripts.sendreceive import SendReceiveData

config = {
    "debug": True,
    "zipFile": "facebook-example",
    "myName": "MyFirst MyLast",
}


if __name__ == "__main__":

    print("====================================")
    print("⛏ Starting the Messenger Analysis")
    print("====================================")

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

    print("====================================")
    print("✓ Messenger Analysis Complete")
    print("====================================")
