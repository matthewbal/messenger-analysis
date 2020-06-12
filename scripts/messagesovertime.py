###################################
# messagesovertime.py
###################################
"""
Author: Matt Balshaw
Start Date: 10/06/2020

Creates a graph of top 20 contacts messages
Also creates an animated video of the messages over time
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.animation as animation
from IPython.display import HTML
import datetime
import os
from pathlib import Path
import sys
import numpy as np
from tqdm import tqdm


class MessagesOverTime():

    def make_graphs(self, config, graphConfig):

        processedLoc = "processed-data/" + config['zipFile'] + "/"
        outpotLoc = "output-data/" + config['zipFile'] + "/"

        Path(outpotLoc).mkdir(parents=True, exist_ok=True)

        allMsgLoc = processedLoc + "allMessageData.csv"
        outputVideo = outpotLoc + "messagesovertime.mp4"
        outputGraph = outpotLoc + "top15contactsbymessages.png"

        if not os.path.exists(allMsgLoc):
            print("Can't find the allMessageData.csv")
            print("☠ Critical error, exiting...")
            sys.exit()

        if os.path.exists(outputVideo) and not config['debug']:
            print("♻ Found existing messagesovertime.mp4")
            return True

        print("Generating a messages over time graph")

        AllMsgs = pd.read_csv(allMsgLoc, usecols=[
                              'name', 'timestamp_ms', 'msg'])

        minTime = AllMsgs['timestamp_ms'].min()
        maxTime = AllMsgs['timestamp_ms'].max()

        totalTime = maxTime - minTime

        ###################
        # Assign colors to groups
        ###################

        metaGroupList = []
        metaColorList = []
        defaultGroupName = ""
        for gName in graphConfig['friendGroups']:
            metaGroupList.append(gName)
            metaColorList.append(graphConfig['friendGroups'][gName]['color'])
            if "default" in graphConfig['friendGroups'][gName] and graphConfig['friendGroups'][gName]['default']:
                defaultGroupName = gName

        fps = graphConfig['fps']
        videoLen = graphConfig['videoLenSecs']

        # 60s video with 5 fps, means we need 5 * 60 frames

        frameTime = int(totalTime / (fps * videoLen))

        maxTime -= (12 * 2592000000)

        frame = 0

        df = pd.DataFrame()

        print("Calculating data for graph...")

        for startTime in tqdm(range(minTime, maxTime, frameTime)):

            endperiod = startTime + frameTime
            maxTime = endperiod + (12 * 2592000000)

            # add one year to end period to stabalize video
            sect = AllMsgs[(AllMsgs['timestamp_ms'] < maxTime) & (
                AllMsgs['timestamp_ms'] > (startTime))].copy()

            # Concatinate sect
            sect['totalMsgs'] = sect.groupby(
                ['name'])['msg'].transform('sum')

            sect = sect.drop_duplicates(subset=['name'])

            # Ignore empty friends for speedup
            # sect = sect[sect['totalMsgs'] > 2]

            if len(sect) < 2:
                continue
            frame += 1

            frameList = []
            groupList = []
            nameList = []
            valueList = []
            timeMinList = []
            timeMaxList = []

            for index, row in sect.iterrows():
                if row['name'] == "Unknown":
                    continue

                # Need to go through each group name
                # Check if our name in that group...
                theName = row['name']
                found = False
                for group in metaGroupList:
                    if theName in graphConfig['friendGroups'][group]['matchList']:
                        groupList.append(group)
                        found = True
                        continue
                if not found:
                    groupList.append(defaultGroupName)

                frameList.append(int(frame))
                nameList.append(row['name'])
                valueList.append(int(row['totalMsgs']))
                dateMin = datetime.datetime.fromtimestamp(
                    (startTime / 1000))
                dateMax = datetime.datetime.fromtimestamp(
                    (maxTime / 1000))
                strMinTime = dateMin.strftime("%b %Y")
                strMaxTime = dateMax.strftime("%b %Y")

                timeMinList.append(strMinTime)
                timeMaxList.append(strMaxTime)

            newRows = {
                "frame": frameList,
                "name": nameList,
                "group": groupList,
                "value": valueList,
                "timeMin": timeMinList,
                "timeMax": timeMaxList,
            }

            newRowsDF = pd.DataFrame.from_dict(newRows)

            df = df.append(newRowsDF)

        # Fix floats to ints
        df['frame'] = df['frame'].apply(np.int64)
        df['value'] = df['value'].apply(np.int64)

        # Create the color index for use in the graph
        colors = dict(zip(
            metaGroupList,
            metaColorList
        ))
        group_lk = df.set_index('name')['group'].to_dict()

        def draw_barchart(frame):

            # If frame is over max, assume we want to render the last frame
            if frame > df['frame'].max():
                frame = df['frame'].max()

            # Get our slice of the graph
            dff = df[df['frame'].eq(frame)].sort_values(
                by='value', ascending=True).tail(graphConfig['maxFriendsShown'])
            # Clear prev graph
            ax.clear()
            # Create bar graph
            ax.barh(
                dff['name'],
                dff['value'],
                color=[colors[group_lk[x]]for x in dff['name']]
            )

            # Plot text on each bar
            dx = dff['value'].max() / 200
            for i, (value, name) in enumerate(zip(dff['value'], dff['name'])):
                # Draw the name
                ax.text(
                    dx,
                    i,
                    name,
                    size=14,
                    weight=600,
                    ha='left',
                    va='center'
                )

                # Draw the value
                ax.text(
                    value + dx,
                    i - .25,
                    f'{value:,.0f}',
                    size=8,
                    ha='left',
                    va='baseline'
                )
                # Draw the group
                ax.text(
                    value - dx,
                    i - .25,
                    group_lk[name],
                    size=8,
                    color='#444444',
                    ha='right',
                    va='baseline'
                )

            # Draw the time
            timeDisp = dff['timeMin'].tail(
                1).item() + " to " + dff['timeMax'].tail(1).item()

            ax.text(
                1,
                0.4,
                timeDisp,
                transform=ax.transAxes,
                color='#777777',
                size=18,
                ha='right',
                weight=800
            )
            # Draw axes label
            ax.text(
                0,
                1.04,
                'Messages Exchanged in 12 month period',
                transform=ax.transAxes,
                size=12,
                color='#777777'
            )

            # Misc formatting
            ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
            ax.xaxis.set_ticks_position('top')
            ax.tick_params(axis='x', colors='#777777', labelsize=12)
            ax.set_yticks([])
            ax.margins(0, 0.01)
            ax.grid(which='major', axis='x', linestyle='-')
            ax.set_axisbelow(True)

            # Set title of the graph
            minTime = df.head(1)['timeMin'].item()
            maxTime = df.tail(1)['timeMax'].item()

            title = "Most Messaged People From %s to %s" % (
                minTime, maxTime)

            ax.text(
                0,
                1.08,
                title,
                transform=ax.transAxes,
                size=16,
                weight=600,
                ha='left'
            )

            # Ensure the graph has adequate spacing
            plt.box(False)
            plt.tight_layout(h_pad=2)
            # Update the progress bar
            pbar.update(1)

        print("Generating animated graph...")

        # Initialise our new graph
        width = graphConfig['width']
        height = graphConfig['height']

        fig, ax = plt.subplots(figsize=(width, height))

        # Calculate intervals per sec for the animator
        # Matplotlib uses ms per frame
        interval = 1000 / fps
        # Calculate extra frames needed for a 5s end of video
        timeAtEnd = 5
        extraframes = fps * timeAtEnd

        # Calculate total frames to display
        numFrames = (df['frame'].max() + extraframes) - df['frame'].min()

        with tqdm(total=numFrames) as pbar:
            animator = animation.FuncAnimation(
                fig,
                draw_barchart,
                interval=interval,
                frames=tqdm(range(df['frame'].min(), df[
                    'frame'].max() + extraframes))
            )
            animator.save(outputVideo)

        print("✓ Messages over time animation complete")
