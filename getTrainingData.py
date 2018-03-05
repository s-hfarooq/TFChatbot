import sqlite3
import pandas as pnds

timeframes = '2018-01' #File number

def makeF(fileName, contentN): #Makes input/output files
    with open("{}.from".format(fileName), 'a', encoding = 'utf8') as f:
        for content in df['{}'.format(contentN)].values:
            f.write(content + '\n')

for timeframe in timeframes: #Starts file process for all db files
    connection = sqlite3.connect('2018-01.db')

    conn = connection.cursor()
    limit = 50
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False

    while cur_length == limit: #Goes until db file fully read
        df = pnds.read_sql("""SELECT * FROM parent_reply WHERE
                                            unix > {} and
                                            parent NOT NULL and
                                            score > 0
                                            ORDER BY unix ASC LIMIT {}""".format(last_unix, limit), connection)

        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)

        if not test_done:
            makeF("test", "parent")
            makeF("test", "comment")
            test_done = True
        else:
            makeF("train", "parent")
            makeF("train", "comment")

        counter += 1

        if counter % 20 == 0:
            print(counter * limit, 'rows completed')
