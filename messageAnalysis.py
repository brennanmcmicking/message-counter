"""
Author: Colin Johnson

This program is functional as of September 2018, future changes to the facebook data
format may break it horribly

"""

import glob, os, json, csv, collections
from pprint import pprint
from datetime import datetime

def readJSON(includeGroups=False):

	# dictionary to store message data
	data = []

	# find appropriate data files
	for filePath in glob.glob("inbox/*/message.json"):
		
		# read all the message.json files
		with open(filePath, 'r') as jsonFile:

			# store data in a new dictionary
			try:
				jsonData = json.load(jsonFile)

				# avoid adding group chats
				if includeGroups or len(jsonData["participants"]) == 2:

					data.append({
						"title": jsonData["title"], 
						"messages": jsonData["messages"], 
						"count": len(jsonData["messages"]),
						"participants": jsonData["participants"]})

			except Exception as e:
				#print("error parsing ", filePath)
				pass

	return data;

def parseData(data, extent, printTotals=False, characters=False):

	# sort and trim data
	data = sorted(data, key=lambda k: k["count"], reverse=True)[:extent]

	# print total message counts
	for convo in data:
		print(convo["count"], " messages from ",  convo["title"]);

	# make a template dictionary of dates between the earlist and most recent messages
	earliestMS = 10e20
	latestMS = 0
	dates = {}

	for convo in data:
		late = convo["messages"][0]["timestamp_ms"]
		early = convo["messages"][len(convo["messages"]) - 1]["timestamp_ms"]
		latestMS = late if late > latestMS else latestMS
		earliestMS = early if early < earliestMS else earliestMS

	earlistStr = datetime.fromtimestamp(earliestMS / 1000).strftime("%B %d %Y")
	latestStr = datetime.fromtimestamp(latestMS / 1000).strftime("%B %d %Y")
	print("from {} to {}.".format(earlistStr, latestStr.strip()))

	for i in range(earliestMS, latestMS, int((latestMS - earliestMS) / 12000)):
		dates[datetime.fromtimestamp(i / 1000).strftime("%B %Y")] = {}

	# add monthly counts for each conversation
	for convo in data:

		# make a deep copy of the template dates dictionary to avoid referencing issues
		datesCopy = dict(dates)
		for key in datesCopy.keys():
			datesCopy[key] = {"sent": 0, "received": 0}
		convo["countsMonthly"] = collections.OrderedDict(datesCopy)

		# count messages/characters by month
		for message in convo["messages"]:
			date = datetime.fromtimestamp(message["timestamp_ms"] / 1000)
			dateStr = date.strftime("%B %Y")

			# increment the count by 1 or by the number of characters in "content"
			content = message.get("content")
			increment = len(content) if (characters and content) else 1

			# track sent and recieved messages separately (you aren't in the participants field)
			if message["sender_name"] in convo["participants"][0]["name"]: # THIS IS BUGGY, ASSUMES SENDER NAME FIRST, BAD FOR GROUP CHATS
				convo["countsMonthly"][dateStr]["received"] += increment
			else:
				convo["countsMonthly"][dateStr]["sent"] += increment

	for i in range(len(data)):
		del data[i]["messages"]

	return data

"""Writes data to fileName.csv in the local directory. If separate is set to true, send messages
and received messages will be counted in separate columns"""
def writeData(data, fileName, separate=False):

	# write data to csv file
	with open(fileName, "w", newline='') as outFile:
		writer = csv.writer(outFile)

		# write header row with titles
		titles = [""]
		for convo in data:
			if separate:
				titles.append(convo["title"] + " - sent")
				titles.append(convo["title"] + " - received")
			else:
				titles.append(convo["title"]) 
		writer.writerow(titles)

		# write data rows
		for date in data[0]["countsMonthly"]:
			rowData = []
			for convo in data:
				counts = convo["countsMonthly"][date]
				if separate:
					rowData.append(counts["sent"])
					rowData.append(counts["received"])
				else:
					rowData.append(int(counts["sent"] + counts["received"]))

			rowData.insert(0, date)
			writer.writerow(rowData)
	return

# get message data and counts from JSON
rawData = readJSON()

# parse the data into a more usable format
data = parseData(rawData, 20, printTotals=True, characters=True)

# write data to a .csv file for excel
fileName = "output.csv"
writeData(data, fileName, separate=False)

#done!
print("Done! Saved to \"{}\".".format(fileName))