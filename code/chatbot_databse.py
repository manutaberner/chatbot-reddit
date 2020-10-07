import sqlite3
import json
from datetime import datetime

timeframe = '2019-01'
sql_transaction = []

connection = sqlite3.connect('{}.db' .format(timeframe))
c = connection.cursor()

def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
                    (parent_id TEXT PRIMARY KEY,
                    comment_id TEXT UNIQUE,
                    parent TEXT,
                    comment TEXT,
                    subreddit TEXT,
                    unix INT,
                    score INT)
                    """)

def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data

def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False  
    elif  data == "[removed]" or data == "[deleted]":
        return False
    else:
        return True


def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = {} LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchnone()
        if result != None:
            return result[0]
        else: 
            return False
    except Exception as e:
        #cleaprint ("find_parent", e)
        return False

def find_exisiting_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = {} LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchnone()
        if result != None:
            return result[0]
        else: 
            return False
    except Exception as e:
        #print ("find_parent", e)
        return False

#Transaction builder
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []
            

def sql_insert_replace_comment(commentid, parentid, parent, comment, subrredit, time, score):
    try:
        sql = """UPDATE parent_reply 
                SET parent_id = ?, comment_id = ?, parent_data = ?, body = ?, subrredit = ?, unix = ?, score = ? 
                WHERE parent_id = ?""".format(commentid, parentid, parent, comment, subrredit, time, score)
        transaction_bldr(sql)
    except Exception as e:
        print ("s-UPDATE", str(e))
        return False

def sql_insert_has_parent(commentid, parentid, parent, comment, subrredit, time, score):
    try:
        sql = """INSERT INTO parent_reply 
                (comment_id, parent_id, parent_data, body, subrredit, unix, score) 
                VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}")""".format(commentid, parentid, parent, comment, subrredit, time, score)
        transaction_bldr(sql)
    except Exception as e:
        print ("s-PARENT insertion", str(e))
        return False

def sql_insert_no_parent(commentid, parentid, comment, subrredit, time, score):
    try:
        sql = """INSERT INTO parent_reply 
                (comment_id, parent_id, body, subrredit, unix, score) 
                VALUES ("{}", "{}", "{}", "{}", "{}", "{}")""".format(commentid, parentid, comment, subrredit, time, score)
        transaction_bldr(sql)
    except Exception as e:
        print ("s-NO_PARENT insertion", str(e))
        return False

if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0
    with open("/Users/manutaberner/GitHub/chatbot/RC_2019-01", buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)   
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = 't1_'+row['id']
            subrredit = row['subreddit']

            parent_data = find_parent(parent_id)

            if score >= 2:
                if acceptable(body):
                    existing_comment_score = find_exisiting_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            #we have existing comment in place but we want to update it because score is higgher
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subrredit, created_utc, score)
                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subrredit, created_utc, score)
                            paired_rows += 1
                        else:
                            #this comment might be anothers comments parent, FIRST COMMENT on thread
                            sql_insert_no_parent(comment_id, parent_id, body, subrredit, created_utc, score)

            if row_counter % 100000 == 0:
                print("Total rows read: {}, paired rows: {}, Time: {}".format(row_counter,paired_rows,str(datetime.now())))
    