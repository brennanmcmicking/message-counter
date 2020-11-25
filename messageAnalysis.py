
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
    '--data',
    help='Location of the messages.json file as provided by Instagram',
    required=True
)

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
filePath = args.data
my_name = args.name
friends = args.friends
outdir = args.outdir

# print(f'Friends: {friends}')

# create the output directory if it does not already exist
if not os.path.exists(outdir):
    os.makedirs(outdir)

# iterate over every message file, grabbing data from each
rows = []

# read the message file into a dictionary
message_file_json = ''
with open(filePath, encoding='UTF-8') as file:
    message_file_json = file.read()
message_file = json.loads(message_file_json)

for thread in message_file:
    # get message file metadata
    participants = thread['participants']
    messages = thread['conversation']

    # print(f'Number of participants: {len(participants)}')

    # create a row for each message
    for message in messages:

        # note: we are also going to include images as a message
        # sending memes to your friends counts.

        # we want to include group chat data in our design, so if we come across a message that our user sent, we have to add a data point for every
        # person that saw it
        # later we coulc change it to showing a line for group chats but we the data doesnt include names and it also stores the same group
        # in multiple threads which makes it difficult to tell if we've already seen this group chat
        if message.get('sender') == my_name:
            # print(f'found sent message to: ')
            for person in participants:
                # print(f'\t{person}')
                if person != my_name:
                    content = message.get('text')
                    rows.append({
                        'friend': person,
                        'received': False,
                        'length': len(content) if content else 0,
                        'timestamp': message.get('created_at')
                    })
        else:
            content = message.get('text')
            rows.append({
                'friend': message.get('sender'),
                'received': True,
                'length': len(content) if content else 0,
                'timestamp': message.get('created_at')
            })

# load the message data into a pandas dataframe
df = pd.DataFrame(rows)
df['timestamp'] = pd.to_datetime(df.timestamp)
df['friend'] = pd.Categorical(df.friend)

# save overall stats for all friends
overall = df.groupby(['friend', 'received']).sum().fillna(0)
overall.to_csv(f'{outdir}all_friends_total.csv')
with open(f'{outdir}all_friends_total.txt', 'w') as fp:
    fp.write(overall.to_string())

if 'all' not in friends:
    # do further analysis with only the specified friends
    df = df[df['friend'].isin(friends)]
df = df.set_index('timestamp')
month_sums = df.groupby([pd.Grouper(freq='M'), 'friend']).count().dropna()
del month_sums['received']
month_sums = month_sums.reset_index().pivot(
    index='timestamp', columns='friend').fillna(0)
month_sums = month_sums['length']

# create stacked area plot
area_plot = month_sums.plot.area(
    figsize=(10, 5), linewidth=0, title='Monthly message counts')
area_plot.legend(title='Friend Name')
area_plot.set_xlabel("x label")
area_plot.set_ylabel("y label")
area_plot.get_figure().savefig(f'{outdir}stacked.png', dpi=300)

# create line plot
line_plot = month_sums.plot.line(lw=1, figsize=(
    10, 5),  title='Monthly message counts')
line_plot.legend(title='Friend Name')
line_plot.set_xlabel("Message Count")
line_plot.set_ylabel("y label")
line_plot.get_figure().savefig(f'{outdir}lines.png', dpi=300)

# create csv file
month_sums.to_csv(f'{outdir}selected_friends_monthly.csv')


def print_friend_list(df):
    for x in df['friend'].unique():
        print(x)
