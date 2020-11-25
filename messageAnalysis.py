
# Standard library imports
import os
import glob
import json
import argparse
from numpy.lib.shape_base import tile

# Third-party imports
import pandas as pd

# Parse command line parameters
parser = argparse.ArgumentParser(description='''
    Process facebook json message data. The messages directory from the data download must 
    be in the current working directory. 
''')
parser.add_argument(
    '--name', 
    help='Name of the owner facebook, as it appear in messages download.',
    required=True
)
parser.add_argument(
    '--friends', 
    nargs='+', 
    help='Name of friends to include in stats.',
    required=True
)
parser.add_argument(
    '--outdir', 
    help='Default output directory for plots',
    default='output/'
)
args = parser.parse_args()
my_name = args.name
friends = args.friends
outdir = args.outdir

# create the output directory if it does not already exist
if not os.path.exists(outdir):
    os.makedirs(outdir)

# iterate over every message file, grabbing data from each
rows = []
for filePath in glob.glob('messages/inbox/**/message_*.json'):

    # read the message file into a dictionary
    message_file_json = ''
    with open(filePath) as file:
        message_file_json = file.read()
    message_file = json.loads(message_file_json)

    # get message file metadata
    participants = message_file['participants']
    messages = message_file['messages']

    # discard group chats
    if len(participants) != 2:
        continue

    # get the name of the friend the current message file is for
    friend_name = participants[0]['name']

    # create a row for each message
    for message in messages:

        # ignore non-text messages e.g. pictures, shares, calls etc.
        msgtype = message.get('type')
        if msgtype != 'Generic':
            continue

        content = message.get('content')
        rows.append({
            'friend': friend_name,
            'sent_by_friend': message.get('sender_name') != my_name,
            'length': len(content) if content else 0,
            'timestamp': message.get('timestamp_ms')
        })

# load the message data into a pandas dataframe
df = pd.DataFrame(rows)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df['friend'] = pd.Categorical(df.friend)

# save overall stats for all friends
overall = df.groupby(['friend', 'sent_by_friend']).sum().fillna(0)
overall.to_csv(f'{outdir}all_friends_total.csv')
with open(f'{outdir}all_friends_total.txt', 'w') as fp:
	fp.write(overall.to_string())

# do further analysis with only the specified friends
df = df[df['friend'].isin(friends)]
df = df.set_index('timestamp')
month_sums = df.groupby([pd.Grouper(freq='M'), 'friend']).count().dropna()
del month_sums['sent_by_friend']
month_sums = month_sums.reset_index().pivot(index='timestamp', columns='friend').fillna(0)
month_sums = month_sums['length']

# create stacked area plot
area_plot = month_sums.plot.area(figsize=(10,5), linewidth=0, title='Monthly message counts')
area_plot.legend(title='Friend Name')
area_plot.set_xlabel("x label")
area_plot.set_ylabel("y label")
area_plot.get_figure().savefig(f'{outdir}stacked.png', dpi=300)

# create line plot
line_plot = month_sums.plot.line(lw=1, figsize=(10,5),  title='Monthly message counts')
line_plot.legend(title='Friend Name')
line_plot.set_xlabel("Message Count")
line_plot.set_ylabel("y label")
line_plot.get_figure().savefig(f'{outdir}lines.png', dpi=300)

# create csv file
month_sums.to_csv(f'{outdir}selected_friends_monthly.csv')

def print_friend_list(df):
	for x in df['friend'].unique():
		print(x)
