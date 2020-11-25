
# Standard library imports
import glob
import json
import argparse

# Third-party imports
import pandas as pd

# Parse command line parameters
parser = argparse.ArgumentParser(description='''
    Process facebook json message data. The messages directory from the data download must 
    be in the current working directory. 
''')
parser.add_argument(
    "--name", 
    help="Name of the owner facebook, as it appear in messages download.",
    required=True
)
parser.add_argument(
    "--friends", 
    nargs="+", 
    help="Name of friends to include in stats.",
    required=True
)
args = parser.parse_args()
my_name = args.name
friends = args.friends

# iterate over every message file, grabbing data from each
rows = []
for filePath in glob.glob("messages/inbox/**/message_*.json"):

    # read the message file into a dictionary
    message_file_json = ""
    with open(filePath) as file:
        message_file_json = file.read()
    message_file = json.loads(message_file_json)

    # get message file metadata
    participants = message_file["participants"]
    messages = message_file["messages"]

    # discard group chats
    if len(participants) != 2:
        continue

    # get the name of the friend the current message file is for
    friend_name = participants[0]["name"]

    # create a row for each message
    for message in messages:

        # ignore non-text messages e.g. pictures, shares, calls etc.
        msgtype = message.get('type')
        if msgtype != 'Generic':
            continue

        sent_by_friend = message.get("sender_name") != my_name
        timestamp = message.get("timestamp_ms")
        content = message.get("content")
        length = len(content) if content else 0

        rows.append({
            'friend': friend_name,
            'sent_by_friend': sent_by_friend,
            'length': length,
            'timestamp': timestamp
        })


df = pd.DataFrame(rows)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit="ms")
df['friend'] = pd.Categorical(df.friend)

for x in df['friend'].unique():
    print(x)
df = df[df['friend'].isin(friends)]

date_index = df.set_index('timestamp')
month_sums = date_index.groupby([pd.Grouper(freq="M"), 'friend']).count().dropna()
del month_sums['sent_by_friend']
month_sums = month_sums.reset_index().pivot(index='timestamp', columns='friend').fillna(0)

print(month_sums.to_string())

ax = month_sums.plot.area(figsize=(10,5), linewidth=0)
ax.get_figure().savefig('stacked.png', dpi=300)

lines = month_sums.plot.line(figsize=(10,5))
lines.get_figure().savefig('lines.png', dpi=300)

month_sums.to_csv('messagedata.csv')
