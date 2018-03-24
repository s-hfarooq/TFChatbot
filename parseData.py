import sqlite3
import json
import time
from datetime import datetime



timeframes = '2015-05' #File name
sql_transaction = []
start_row = 0
cleanup = 1000000 #Frequency of cleanup
bufferAmt = 400000 #How many comments get added to the buffer at one time

connection = sqlite3.connect('{}.db'.format(timeframes))
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



def trans_bldr(sql): #Sends the transaction once it's ready
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > bufferAmt:
        conn.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                conn.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []



def sql_i_HP(commentid, parentid, parent, comment, subreddit, time, score): #Inserts comment if a parent exists
    try:
        sql = """INSERT INTO parent_reply (parent_id,
                                comment_id,
                                parent,
                                comment,
                                subreddit,
                                unix,
                                score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        trans_bldr(sql)
    except Exception as e:
        print('s-HAS_PARENT insertion', str(e))



def sql_i_NP(commentid, parentid, comment, subreddit, time, score): #Inserts comment if no parent exists
    try:
        sql = """INSERT INTO parent_reply (parent_id,
                                comment_id,
                                comment,
                                subreddit,
                                unix,
                                score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        trans_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT insertion', str(e))



def sql_i_RC(commentid, parentid, parent, comment, subreddit, time, score): #Inserts comment otherwise
    try:
        sql = """UPDATE parent_reply SET parent_id = ?,
                                comment_id = ?,
                                parent = ?,
                                comment = ?,
                                subreddit = ?,
                                unix = ?,
                                score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        trans_bldr(sql)
    except Exception as e:
        print('s-RC insertion', str(e))



def goodData(data): #Makes sure comments aren't removed and aren't too short/long
    if data == '[removed]' or data == '[deleted]':
        return False
    elif len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    else:
        return True



def find_parent(pid): #Searches for parent, returns the result if one is found
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        conn.execute(sql)
        result = conn.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        print("ERROR (find_parent): ", str(e))
        return False



def find_score(pid): #Gets score of comment
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        conn.execute(sql)
        result = conn.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("ERROR (find_score): ", str(e))
        return False






if __name__ == '__main__': #Execute if this is the main file
    row_counter = 0
    paired_rows = 0

    create_table()

    with open("D:\GitHub\TFChatbot\RC_{}".format(timeframes.split('-')[0], timeframes), buffering = bufferAmt) as f:
        #Starts inputting data into sql table with correct formatting

        for row in f:
            row_counter += 1

            if row_counter > start_row:
                try:
                    row = json.loads(row)
                    parent_id = row['parent_id'].split('_')[1]
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    score = row['score']

                    comment_id = row['id']

                    subreddit = row['subreddit']
                    parent_data = find_parent(parent_id)

                    existing_comment_score = find_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score and goodData(body):
                                sql_i_RC(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if goodData(body):
                            if parent_data:
                                if score >= 3: #Only insert comments that have enough upvotes
                                    sql_i_HP(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                    paired_rows += 1
                            else:
                                sql_i_NP(comment_id, parent_id, body, subreddit, created_utc, score)
                except Exception as e:
                    print(str(e))

            if row_counter % 100000 == 0: #Prints progress
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))

            if row_counter > start_row: #Removes comments with null parents, comment out if you want all comments
                if row_counter % cleanup == 0:
                    print("Cleaning")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    conn.execute(sql)
                    connection.commit()
                    conn.execute("VACUUM")
                    connection.commit()
