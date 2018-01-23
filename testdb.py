# -*- coding: utf-8 -*-
import psycopg2
import time
import datetime
conn = psycopg2.connect(dbname="isv_zendesk",
                        user="postgres",
                        password="Dxf3529!",
                        host="127.0.0.1",
                        port="5432")
cur = conn.cursor()
# TO-DO exception handler when table not exists
cur.execute("select * from isv_posts_json")


posts = cur.fetchall()
# posts = [({})]
"""
id :             <class 'int'>
url :            <class 'str'>
title :          <class 'str'>
closed :         <class 'bool'>
pinned :         <class 'bool'>
status :         <class 'str'>
details :        <class 'str'>
featured :       <class 'bool'>
html_url :       <class 'str'>
topic_id :       <class 'int'>
vote_sum :       <class 'int'>
author_id :      <class 'int'>
created_at :     <class 'str'>
updated_at :     <class 'str'>
vote_count :     <class 'int'>
comment_count :  <class 'int'>
follower_count : <class 'int'>
"""

cur.execute("""
CREATE TABLE IF NOT EXISTS isv_posts (
id varchar PRIMARY KEY, 
url varchar, 
title text, 
closed bool, 
pinned bool, 
status varchar, 
details text, 
featured bool, 
html_url varchar, 
topic_id varchar, 
vote_sum integer, 
author_id varchar, 
created_at_timestamp real,
created_at_str varchar, 
updated_at_timestamp real,
updated_at_str varchar, 
vote_count integer, 
comment_count integer, 
follower_count integer
);
"""
            )
'''
command = """
INSERT INTO isv_posts (
id, url, title, closed, pinned, status, details, 
featured, html_url, topic_id, vote_sum, author_id, 
created_at, updated_at, vote_count, comment_count, follower_count) 
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
'''
for p in posts:
    print('-'*20)
    post = p[0]
    created_at = post['created_at']
    created_at_date = created_at[:10]
    created_at_time = created_at[11:-1]
    created_at = created_at_date + ' ' + created_at_time
    t = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
    created_at_timestamp = time.mktime(t)
    print(created_at,created_at_timestamp)

    updated_at = post['updated_at']
    updated_at_date = updated_at[:10]
    updated_at_time = updated_at[11:-1]
    updated_at = updated_at_date + ' ' + updated_at_time
    t = time.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
    updated_at_timestamp = time.mktime(t)


    command = "INSERT INTO isv_posts VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cur.execute(command, (str(post['id']),
                          post['url'],
                          post['title'],
                          post['closed'],
                          post['pinned'],
                          post['status'],
                          post['details'],
                          post['featured'],
                          post['html_url'],
                          str(post['topic_id']),
                          post['vote_sum'],
                          str(post['author_id']),
                          created_at_timestamp,
                          created_at,
                          updated_at_timestamp,
                          updated_at,
                          post['vote_count'],
                          post['comment_count'],
                          post['follower_count']))



conn.commit()
cur.close()
conn.close()
