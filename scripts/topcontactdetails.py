###################################
# topcontactdetails.py
###################################
"""
Author: Matt Balshaw
Start Date: 12/06/2020

Outputs graphs about the top contacts
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import sys
from tqdm import tqdm
from pathlib import Path


class TopContactDetails():

    def generate_all_graphs(self, config):

        # Checks that we have the allMessagesData

        processedLoc = "processed-data/" + config['zipFile'] + "/"
        outpotLoc = "output-data/" + config['zipFile'] + "/"

        Path(outpotLoc).mkdir(parents=True, exist_ok=True)

        allMsgLoc = processedLoc + "allMessageData.csv"
        outputFilePath = outpotLoc + "topContactMetrics.csv"

        if not os.path.exists(allMsgLoc):
            print("Can't find the allMessageData.csv")
            print("☠ Critical error, exiting...")
            sys.exit()

        if os.path.exists(outputFilePath) and not config['debug']:
            print("♻ Found existing sendreceivecontacts.csv")
            return True

        print("Calculating our top contact data...")

        allMessages = pd.read_csv(allMsgLoc)

        # Get characters of each message
        allMessages['len'] = allMessages.apply(
            lambda row: len(row.content), axis=1)

        # Calculate average length of messages
        allMessages['averageLen'] = allMessages.groupby(
            ['name'])['len'].transform('mean')

        # Count length of messages received and sent
        allMessages['partLen'] = allMessages.groupby(['name', 'received'])[
            'len'].transform('sum')

        # Calculate total length of messages
        allMessages['totalLen'] = allMessages.groupby(
            ['name'])['len'].transform('sum')

        # Calculate total messages
        allMessages['msg'] = 1
        allMessages['totalMsgs'] = allMessages.groupby(
            ['name'])['msg'].transform('sum')

        # Calculate longest message
        allMessages['longestMsg'] = allMessages.groupby(['name'])[
            'len'].transform('max')

        print("Calculating conversation metrics")

        # Calculate total conv starts
        allMessages['convsTheyStarted'] = allMessages.groupby(
            ['name', 'received'])['startedConv'].transform('sum')
        allMessages['totalConvs'] = allMessages.groupby(
            ['name'])['startedConv'].transform('sum')

        #######################################################
        # Drop all duplicates to speed up following calculations
        allMessages = allMessages.drop_duplicates(
            subset=['name', 'received'])

        #######################################################

        # Ignore all the sent messages
        allMessages = allMessages[allMessages['received'] == 1]

        # Calculate percentage of messages that were received
        allMessages['pctReceived'] = allMessages.apply(
            lambda row: (row.partLen / row.totalLen), axis=1)

        # Calculate messages per conversation
        allMessages['msgsPerConv'] = allMessages.apply(
            lambda row: (row.totalMsgs / row.totalConvs), axis=1)

        # Calculate percentage of conversations they started
        allMessages['pctStarted'] = allMessages.apply(
            lambda row: (row.convsTheyStarted / row.totalConvs), axis=1)
        print("Saving raw data")

        # Drop intermediary cols
        allMessages = allMessages.drop(
            columns=["Unnamed: 0", "received", "startedConv", "content", "type", "timestamp_ms", "len"])

        # Sort by total chars sent and received
        allMessages = allMessages.sort_values(
            by=['totalLen'], ascending=False)
        #######################################################
        # Save final csv
        allMessages.to_csv(outputFilePath)

        # Format for graphs
        numContacts = config['topContactDetails']['maxFriendsShown']

        graphData = allMessages.head(numContacts).copy()

        # Format rows
        graphData['pctReceived'] = round(
            graphData['pctReceived'] * 100.,
            2
        )
        graphData['averageLen'] = round(
            graphData['averageLen'],
            1
        )
        graphData['pctStarted'] = round(
            graphData['pctStarted'],
            2
        )
        graphData['msgsPerConv'] = round(
            graphData['msgsPerConv'],
            1
        )

        #######################################################
        # Do the percent message graph
        ################

        graphData = graphData.sort_values(
            by=['pctReceived'], ascending=True)

        graphConfig = {
            "title": "Top %s contacts ordered by percentage characters sent to me" % numContacts,
            "xTitle": "Percentage characters sent to me",
            'xaxis': 'pctReceived',
            'yaxis': 'name',
            "units": "%",
            "saveFile": outpotLoc + "topContactsReceivedPercentage.svg",
            "width": config['topContactDetails']['width'],
            "height": config['topContactDetails']['height']

        }

        self.make_graph(graphData, graphConfig)

        ################
        # Do the average length graph
        ################
        graphData = graphData.sort_values(
            by=['averageLen'], ascending=True)

        graphConfig = {
            "title": "Top %s contacts ordered by average message length" % numContacts,
            "xTitle": "Average message length in characters",
            "xaxis": "averageLen",
            "yaxis": "name",
            "units": " chars",
            "saveFile": outpotLoc + "topContactsAverageMessageLength.svg",
            "width": config['topContactDetails']['width'],
            "height": config['topContactDetails']['height']
        }

        self.make_graph(graphData, graphConfig)

        ################
        # Do the conversations started graph
        ################
        graphData = graphData.sort_values(
            by=['pctStarted'], ascending=True)

        graphConfig = {
            "title": "Top %s contacts ordered by number conversations they started" % numContacts,
            "xTitle": "Percentage conversations they started",
            "xaxis": "pctStarted",
            "yaxis": "name",
            "units": "%",
            "saveFile": outpotLoc + "topContactsConversationsStarted.svg",
            "width": config['topContactDetails']['width'],
            "height": config['topContactDetails']['height']
        }

        self.make_graph(graphData, graphConfig)

        ################
        # Do the messages per conversation
        ################
        graphData = graphData.sort_values(
            by=['msgsPerConv'], ascending=True)

        graphConfig = {
            "title": "Top %s contacts ordered by average messages per conversation" % numContacts,
            "xTitle": "Average messages per conversation",
            "xaxis": "msgsPerConv",
            "yaxis": "name",
            "units": " messages",
            "saveFile": outpotLoc + "topContactsMessagesPerConversation.svg",
            "width": config['topContactDetails']['width'],
            "height": config['topContactDetails']['height']
        }

        self.make_graph(graphData, graphConfig)
        #######################################################
        print("✓ Top contacts data Graphed")

    def make_graph(self, graphData, graphConfig):

        # Plot the graph

        fig, ax = plt.subplots(
            figsize=(graphConfig['width'], graphConfig['height']))

        # Create bar graph
        ax.barh(
            graphData[graphConfig['yaxis']],
            graphData[graphConfig['xaxis']],
            color='#90d595'
        )
        # Plot text on each bar
        dx = graphData[graphConfig['xaxis']].max() / 200
        for i, (value, name) in enumerate(zip(graphData[graphConfig['xaxis']], graphData['name'])):
            # Draw the name
            ax.text(
                dx,
                i,
                name,
                size=14,
                weight=400,
                ha='left',
                va='center'
            )

            # Draw the value and group
            ax.text(
                value + dx,
                i - .25,
                str(value) + graphConfig['units'],
                size=8,
                ha='left',
                va='baseline'
            )

        # Draw axes label
        ax.text(
            0,
            1.02,
            graphConfig['xTitle'],
            transform=ax.transAxes,
            size=12,
            color='#777777'
        )

        # Draw title
        ax.text(
            0,
            1.04,
            graphConfig['title'],
            transform=ax.transAxes,
            size=16,
            weight=600,
            ha='left'
        )

        # Misc formatting
        ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        ax.xaxis.set_ticks_position('top')
        ax.tick_params(axis='x', colors='#777777', labelsize=12)
        ax.set_yticks([])
        ax.margins(0, 0.01)
        ax.grid(which='major', axis='x', linestyle='-')
        ax.set_axisbelow(True)

        plt.box(False)
        plt.tight_layout(h_pad=3)

        plt.savefig(graphConfig['saveFile'])
