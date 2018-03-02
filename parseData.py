import sqlite3
import json
from datetime import datetime

timeframe = '2018-01' #File number
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
conn = connection.cursor()

def create_table(): #Creates table with correct elements if it doesn't already exist
    conn.execute("""CREATE TABLE IF NOT EXISTS parent_reply(
                                parent_id TEXT PRIMARY KEY,
                                comment_id TEXT UNIQUE,
                                parent TEXT,
                                comment TEXT,
                                subreddit TEXT,
                                unix INT,
                                score INT)""")

def format_data(data): #Formats the data before it's entered into the table
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data

def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 5000:
        conn.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                conn.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

def sql_i_RC(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?,
                                comment_id = ?,
                                parent = ?,
                                comment = ?,
                                subreddit = ?,
                                unix = ?,
                                score = ?
                                WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s-update insertion', str(e))

def sql_i_HP(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (
                            parent_id,
                            comment_id,
                            parent,
                            comment,
                            subreddit,
                            unix,
                            score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-has_parent insertion', str(e))

def sql_i_NP(commentid, parentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id,
                                        comment_id,
                                        comment,
                                        subreddit,
                                        unix,
                                        score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-no_parent insertion', str(e))

def goodData(data):
    if data == '[removed]' or data == '[deleted]':
        return False
    elif len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    else:
        return True

def find_parent(pid): #Searches for parent, returns the result if one is found
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        conn.execute(sql)
        result = conn.fetchone()
        if result == None:
            return False
        else:
            return result[0]
    except Exception as e:
        return False

def find_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        conn.execute(sql)
        result = conn.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False

if __name__ == '__main__': #Execute if this is the main file
    row_counter = 0 #How many rows it's gone through
    paired_rows = 0 #Parent/child pairs
    create_table()


    with open("D:/GitHub/TFChatbot/RC_{}".format(timeframe.split('-') [0], timeframe), buffering = 5000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['link_id']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score >= 3: #Only insert comments that have enough upvotes
                existing_comment_score = find_score(parent_id)
                if existing_comment_score:
                    if score > existing_comment_score:
                        if goodData(body):
                            sql_i_RC(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                else:
                    if goodData(body):
                        if parent_data:
                            sql_i_HP(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_i_NP(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 100000 == 0: #Show counter of progress in terminal
                print('Total rows done: {}, Paired rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))
