import sqlite3
import json
from datetime import datetime

timeframe = '2018-01' #File number
sql_transacton = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def create_table(): #Creates table with correct elements if it doesn't already exist
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply(
                parent_id TEXT PRIMARY KEY,
                comment_id TEXT UNIQUE,
                parent TEXT, comment TEXT,
                subreddit TEXT,
                unix INT,
                score INT)""")

def format_data(data): #Formats the data before it's entered into the table
    data = data.replace("\n", " newlinecharacter ").replace("\r", " newlinecharacter ").replace('""', "''")
    return data


def find_parent(idpar): #Searches for parent, returns the result if one is found
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(idpar)
        c.execute(sql)
        result = c.fetchone()

        if result != None:
            return result[0]
        else:
            return False

    except Exception as e:
        print ("ERROR - find_parent ", e)
        return False

def find_existing_score(idpar):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(idpar)
        c.execute(sql)
        result = c.fetchone()

        if result != None:
            return result[0]
        else:
            return False

    except Exception as e:
        print ("ERROR - find_parent ", e)
        return False


def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[removed]' or data == '[deleted]':
        return False
    else:
        return True


if __name__ == "__main__": #Execute if this is the main file
    row_counter = 0 #How many rows it's gone through
    create_table()
    paired_rows = 0 #Parent/child pairs

    with open("D:/GitHub/TFChatbot/RC_{}".format(timeframe.split('-')[0], timeframe), buffering = 1000) as f:
        #Starts inputting data into sql table with correct formatting
        for row in f:
            print(row)
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)

            if score >= 3: #only insert comments that have enough updoots
                existing_comment_score = find_existing_score(parent_id)

                if existing_comment_score:
                    if score > existing_comment_score:
                        print ("hello")
