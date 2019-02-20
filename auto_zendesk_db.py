#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
Copyright 2018 Francis Xufan Du - BEYONDSOFT INC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Created on Thu Jan 18 14:06:16 2018

@author: Francis Xufan Du - BEYONDSOFT INC.
@email: duxufan@beyondsoft.com xufan.du@gmail.com
@Version: 	06/2018 0.8-Beta    add new auto crawler to use zendesk api instead of using browser(need zendesk agent)
            03/2018 0.7-Beta    add auto_zendesk_report.py module to generate reports based on MarkDown
            03/2018 0.6.5-Beta add isv_status in database table isv_posts, to record post status marked by isv team
            03/2018 0.6-Beta:   1. update the tool to only collect the necessary data
                                2. change database updating logic (old way: delete all and re-create new table,
                                new way: update or insert)
                                3. fix bugs
            02/2018 0.5-Beta: separate crawling logic and database logic
            02/2018 0.4-Beta: add database update recording
            02/2018 0.3-Beta: add users and topics data collecting
            01/2018 0.2-Beta: add database storage
            01/2018 0.1-Beta: build zendesk auto collect function
"""

# core mods
import os
import json
import xlwt
import time
import datetime
import re

# 3rd party mods
import psycopg2
import psycopg2.extras
import configure


class AutoZendeskDB(object):
    def __init__(self, postgresql_dbname, postgresql_user, postgresql_passwd, postgresql_host, postgresql_port):
        """
        initial method
        :param postgresql_dbname: database name
        :param postgresql_user: database user name
        :param postgresql_passwd: passwd for the user
        :param postgresql_host: database host
        :param postgresql_port: database port
        """
        self._save_path = configure.OUTPUT_PATH

        # length limit of a excel cell
        self._EXCEL_MAXIMUM_CELL = 32767
        self._posts_id = []
        self._tickets_id = []

        self._postgresql_dbname = postgresql_dbname
        self._postgresql_user = postgresql_user
        self._postgresql_passwd = postgresql_passwd
        self._postgresql_host = postgresql_host
        self._postgresql_port = postgresql_port
        self._postgresql_conn = None

        self._json_posts_filename_list = []
        self._json_comments_filename_list = []
        self._json_users_filename_list = []
        self._json_tickets_filename_list = []
        self._json_tickets_comments_filename_list = []

        self._connect_postgresql()

    def __del__(self):
        self._disconnect_postgresql()
        super()

    def _connect_postgresql(self):
        """
        connect to postgresql database.
        :return: None
        """
        self._postgresql_conn = psycopg2.connect(dbname=self._postgresql_dbname,
                                                 user=self._postgresql_user,
                                                 password=self._postgresql_passwd,
                                                 host=self._postgresql_host,
                                                 port=self._postgresql_port)
        print("Connected to {host}:{port}  {db}".format(host=self._postgresql_host,
                                                        port=self._postgresql_port,
                                                        db=self._postgresql_dbname))

    def _disconnect_postgresql(self):
        """
        disconnect database.
        :return: None
        """
        self._postgresql_conn.close()
        print("Disconnect database")

    def _drop_all_table_postgresql(self):
        """
        drop all posts and comments tables in database
        :return: None
        """

        cur = self._postgresql_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS isv_posts;")

        cur.execute("DROP TABLE IF EXISTS isv_comments;")
        cur.execute("DROP TABLE IF EXISTS isv_comments_json;")
        self._postgresql_conn.commit()
        cur.close()

    def _initial_posts_postgresql(self):
        """
        build post json table
        :return: None
        """
        self._build_json_posts_file_list()
        self._parse_json_posts_file()

        cur = self._postgresql_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS isv_posts_json (id VARCHAR PRIMARY KEY, jdoc jsonb);")

        for filename in self._json_posts_filename_list:
            data = self._load_json(filename)
            posts = data['posts']
            for post in posts:
                cur.execute("SELECT * FROM isv_posts_json WHERE id = %s;", (str(post['id']),))
                result = cur.fetchall()
                if result:
                    command = "UPDATE isv_posts_json SET jdoc = %s WHERE id = %s;"
                    cur.execute(command, [psycopg2.extras.Json(post), str(post['id'])])
                else:
                    cur.execute("INSERT INTO isv_posts_json (id, jdoc) VALUES(%s, %s);",
                                [str(post['id']), psycopg2.extras.Json(post)])

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_posts_json updated")

    def _parse_json_posts_file(self):
        for file in self._json_posts_filename_list:
            try:
                with open(file, 'r', encoding='utf8') as f:
                    data = json.load(f)
                    posts = data['posts']
                    for post in posts:
                        self._posts_id.append(str(post['id']))

            except json.JSONDecodeError:
                print("ERROR: Json file {0} decode error!".format(file))
                quit()

    def _build_json_posts_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^post.*.json', file):
                    self._json_posts_filename_list.append(os.path.join(self._save_path, file))

    def _build_json_users_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^users.*.json', file):
                    self._json_users_filename_list.append(os.path.join(self._save_path, file))

    def _build_json_comments_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^comment.*.json', file):
                    self._json_comments_filename_list.append(os.path.join(self._save_path, file))

    def _initial_comments_postgresql(self):
        """
        build comment json table
        :return: None
        """
        self._build_json_comments_file_list()

        cur = self._postgresql_conn.cursor()
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS isv_comments_json (
                    id VARCHAR PRIMARY KEY,
                    post_id VARCHAR,
                    jdoc jsonb);
                    """)

        for filename in self._json_comments_filename_list:
            data = self._load_json(filename)
            comments = data['comments']
            for comment in comments:
                cur.execute("SELECT * FROM isv_comments_json WHERE id=%s;", (str(comment['id']),))
                result = cur.fetchall()
                if result:
                    command = "UPDATE isv_comments_json SET jdoc = %s WHERE id = %s;"
                    cur.execute(command, [psycopg2.extras.Json(comment), str(comment['id'])])
                else:
                    command = "INSERT INTO isv_comments_json (id, post_id, jdoc) VALUES(%s, %s, %s);"
                    cur.execute(command, [str(comment['id']), str(comment['post_id']), psycopg2.extras.Json(comment)])

        self._postgresql_conn.commit()
        cur.close()

        print("table isv_comments_json updated")

    def _build_topics_postgresql(self):

        """
        build isv_topics table in database
        :return: None
        """
        cur = self._postgresql_conn.cursor()
        """
        "id": varchar
        "url": varchar
        "html_url": varchar
        "name": varchar
        "description: varchar
        "position": varchar
        "follower_count": varchar
        "community_id": varchar
        "created_at": varchar
        "updated_at": varchar
        "user_segment_id": varchar
        """
        cur.execute("""
        CREATE TABLE IF NOT EXISTS isv_topics (
        id varchar PRIMARY KEY,
        url varchar,
        html_url varchar,
        name varchar,
        description varchar,
        position varchar,
        follower_count varchar,
        community_id varchar,
        created_at varchar,
        updated_at varchar,
        user_segment_id varchar   
        );
        """
                    )
        try:
            data = self._load_json(os.path.join(self._save_path, 'topics.json'))
        except IOError:
            print("ERROR: IO ERROR when load {0}".format(self._save_path + r'\topics.json'))
            quit()
        except json.JSONDecodeError:
            print("ERROR: Json file {0} decode error!".format(self._save_path + r'\topics.json'))
            quit()

        topics = data['topics']
        for topic in topics:
            created_at = topic['created_at']
            created_at_date = created_at[:10]
            created_at_time = created_at[11:-1]
            created_at = created_at_date + ' ' + created_at_time

            updated_at = topic['updated_at']
            updated_at_date = updated_at[:10]
            updated_at_time = updated_at[11:-1]
            updated_at = updated_at_date + ' ' + updated_at_time

            cur.execute("SELECT * FROM isv_topics WHERE id = %s;", (str(topic['id']),))
            result = cur.fetchall()
            # if record already exists, update it
            # or insert the new record
            if result:
                command = """UPDATE isv_topics SET url = %s, 
                          html_url = %s,
                          name = %s,
                          description = %s,
                          position = %s,
                          follower_count = %s,
                          community_id = %s,
                          created_at = %s,
                          updated_at = %s,
                          user_segment_id = %s WHERE id = %s;
                          """
                cur.execute(command, (topic['url'],
                                      topic['html_url'],
                                      topic['name'],
                                      topic['description'],
                                      str(topic['position']),
                                      str(topic['follower_count']),
                                      str(topic['community_id']),
                                      created_at,
                                      updated_at,
                                      str(topic['user_segment_id']),
                                      str(topic['id'])
                                      ))

            else:
                command = "INSERT INTO isv_topics VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                cur.execute(command, (str(topic['id']),
                                      topic['url'],
                                      topic['html_url'],
                                      topic['name'],
                                      topic['description'],
                                      str(topic['position']),
                                      str(topic['follower_count']),
                                      str(topic['community_id']),
                                      created_at,
                                      updated_at,
                                      str(topic['user_segment_id'])
                                      ))

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_topics updated")

    def _build_users_postgresql(self):
        """

        :return:
        """
        self._build_json_users_file_list()
        cur = self._postgresql_conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS isv_users (
        id varchar PRIMARY KEY,
        url varchar,
        name varchar,
        email varchar,
        created_at varchar,
        updated_at varchar,
        time_zone varchar,
        phone varchar,
        shared_phone_number varchar,
        photo text,
        locale_id varchar,
        locale varchar,
        organization_id varchar,
        role varchar,
        verified bool,
        last_login_at VARCHAR,
        restricted_agent bool
        );
        """
                    )

        for filename in self._json_users_filename_list:
            try:
                data = self._load_json(filename)
            except IOError:
                print("ERROR: IO ERROR when load {0}".format(filename))
                quit()
            except json.JSONDecodeError:
                print("ERROR: Json file {0} decode error!".format(filename))
                quit()

            users = data['users']
            for user in users:
                # TODO: why 3 tickets here?
                if 'body' in user.keys():
                    continue

                created_at = user['created_at']
                created_at_date = created_at[:10]
                created_at_time = created_at[11:-1]
                created_at = created_at_date + ' ' + created_at_time

                updated_at = user['updated_at']
                updated_at_date = updated_at[:10]
                updated_at_time = updated_at[11:-1]
                updated_at = updated_at_date + ' ' + updated_at_time

                cur.execute("SELECT * FROM isv_users WHERE id = %s;", (str(user['id']),))
                result = cur.fetchall()
                # TODO
                print(user)

                if result:
                    command = """UPDATE isv_users SET url = %s,
                                name = %s,
                                email = %s,
                                created_at = %s,
                                updated_at = %s,
                                time_zone = %s,
                                shared_phone_number = %s,
                                locale_id = %s,
                                locale = %s,
                                organization_id = %s,
                                role = %s,
                                verified = %s,
                                last_login_at = %s,
                                restricted_agent = %s,  
                                """
                    cur.execute(command, (
                        user['url'],
                        user['name'],
                        user['email'],
                        created_at,
                        updated_at,
                        user['time_zone'],
                        str(user['shared_phone_number']),
                        str(user['locale_id']),
                        user['locale'],
                        str(user['organization_id']),
                        user['role'],
                        user['verified'],
                        str(user['last_login_at']),
                        user['restricted_agent'],
                        str(user['id'])
                    ))
                else:
                    command = "INSERT INTO isv_users VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    a = (str(user['id']),
                                          user['url'],
                                          user['name'],
                                          user['email'],
                                          created_at,
                                          updated_at,
                                          user['time_zone'],
                                          str(user['shared_phone_number']),
                                          str(user['locale_id']),
                                          user['locale'],
                                          str(user['organization_id']),
                                          user['role'],
                                          user['verified'],
                                          str(user['last_login_at']),
                                          user['restricted_agent']
                                          )
                    print(type(a))
                    print(len(a))
                    print('A:', a)
                    cur.execute(command, a)

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_users updated")

    def _build_posts_postgresql(self):
        """
        build post table
        :return: None
        """

        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select jdoc from isv_posts_json")
        except Exception:
            print("Error, check if table isv_posts_json exists")

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
        follower_count integer,
        isv_status VARCHAR
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
            post = p[0]
            created_at = post['created_at']
            created_at_date = created_at[:10]
            created_at_time = created_at[11:-1]
            created_at = created_at_date + ' ' + created_at_time
            t = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at_timestamp = time.mktime(t)

            updated_at = post['updated_at']
            updated_at_date = updated_at[:10]
            updated_at_time = updated_at[11:-1]
            updated_at = updated_at_date + ' ' + updated_at_time
            t = time.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
            updated_at_timestamp = time.mktime(t)

            cur.execute("SELECT * FROM isv_posts WHERE id = %s;", (str(post['id']),))
            result = cur.fetchall()
            if result:
                command = """UPDATE isv_posts SET url = %s,
                            title = %s, 
                            closed = %s, 
                            pinned = %s, 
                            status = %s, 
                            details = %s, 
                            featured = %s, 
                            html_url = %s, 
                            topic_id = %s, 
                            vote_sum = %s, 
                            author_id = %s, 
                            created_at_timestamp = %s,
                            created_at_str = %s, 
                            updated_at_timestamp = %s,
                            updated_at_str = %s, 
                            vote_count = %s, 
                            comment_count = %s, 
                            follower_count = %s WHERE id = %s;
                            """
                cur.execute(command, (
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
                    post['follower_count'],
                    str(post['id'])
                ))
            else:
                command = """
                          INSERT INTO isv_posts(
                                    id, 
                                    url, 
                                    title, 
                                    closed, 
                                    pinned, 
                                    status, 
                                    details, 
                                    featured, 
                                    html_url, 
                                    topic_id, 
                                    vote_sum, 
                                    author_id, 
                                    created_at_timestamp,
                                    created_at_str, 
                                    updated_at_timestamp,
                                    updated_at_str, 
                                    vote_count, 
                                    comment_count, 
                                    follower_count
                          )                           
                          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
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

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_posts updated")

    def _build_comments_postgresql(self):
        """
        build comment table
        :return: None
        """
        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select jdoc from isv_comments_json")
        except Exception:
            print("Error, check if table isv_comments_json exists")
        comments = cur.fetchall()
        # comments = [({})]
        """
        id :             <class 'int'>
        url :            <class 'str'>
        body :           <class 'str'>
        post_id :        <class 'int'>
        html_url :       <class 'str'>
        official :       <class 'bool'>        
        vote_sum :       <class 'int'>
        author_id :      <class 'int'>                    
        created_at :     <class 'str'>
        updated_at :     <class 'str'> 
        vote_count :     <class 'int'>       

        """

        cur.execute("""
        CREATE TABLE IF NOT EXISTS isv_comments (
        id varchar PRIMARY KEY, 
        url varchar, 
        body text,
        post_id varchar, 
        html_url varchar, 
        official bool,
        vote_sum integer, 
        author_id varchar, 
        created_at_timestamp real,
        created_at_str varchar, 
        updated_at_timestamp real,
        updated_at_str varchar, 
        vote_count integer
        );
        """)

        for c in comments:
            comment = c[0]
            created_at = comment['created_at']
            created_at_date = created_at[:10]
            created_at_time = created_at[11:-1]
            created_at = created_at_date + ' ' + created_at_time
            t = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at_timestamp = time.mktime(t)

            updated_at = comment['updated_at']
            updated_at_date = updated_at[:10]
            updated_at_time = updated_at[11:-1]
            updated_at = updated_at_date + ' ' + updated_at_time
            t = time.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
            updated_at_timestamp = time.mktime(t)

            cur.execute("SELECT * FROM isv_comments WHERE id=%s;", (str(comment['id']),))
            result = cur.fetchall()
            if result:
                command = """UPDATE isv_comments SET url = %s,
                                body = %s,
                                post_id = %s, 
                                html_url = %s, 
                                official = %s,
                                vote_sum = %s, 
                                author_id = %s, 
                                created_at_timestamp = %s,
                                created_at_str = %s, 
                                updated_at_timestamp = %s,
                                updated_at_str = %s, 
                                vote_count = %s WHERE id = %s;
                            """
                cur.execute(command, (
                                      comment['url'],
                                      comment['body'],
                                      str(comment['post_id']),
                                      comment['html_url'],
                                      comment['official'],
                                      comment['vote_sum'],
                                      str(comment['author_id']),
                                      created_at_timestamp,
                                      created_at,
                                      updated_at_timestamp,
                                      updated_at,
                                      comment['vote_count'],
                                      str(comment['id']),
                ))
            else:
                command = "INSERT INTO isv_comments VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                cur.execute(command, (str(comment['id']),
                                      comment['url'],
                                      comment['body'],
                                      str(comment['post_id']),
                                      comment['html_url'],
                                      comment['official'],
                                      comment['vote_sum'],
                                      str(comment['author_id']),
                                      created_at_timestamp,
                                      created_at,
                                      updated_at_timestamp,
                                      updated_at,
                                      comment['vote_count'],))

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_comments updated")

    @staticmethod
    def _load_json(filename):
        """
        load json file
        :param filename: file name of json file need to load
        :return: raw data loaded from json file
        """
        try:
            with open(filename, encoding='utf8') as json_file:
                data = json.load(json_file)
                return data
        except json.JSONDecodeError:
            print("Error: Json file {0} decode error!".format(filename))
            quit()

    def _build_excel_filename(self, data_type):
        """
        Build excel file name, and remove excel if already exists
        :param data_type: 'posts' or 'comments' or 'tickets' or 'ticket_comments'
        :return: excel filename
        """
        local_time = time.strftime("_%Y_%m_%d", time.localtime(time.time()))
        save_filename = os.path.join(self._save_path, data_type + local_time + '.xls')
        if os.path.exists(save_filename):
            os.remove(save_filename)

        return save_filename

    def build_posts_excel_from_db(self):
        """
        build Excel report file from collected json files
        """
        row_cnt = 0
        workbook = xlwt.Workbook()
        ws = workbook.add_sheet("All posts")
        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select jdoc from isv_posts_json")
        except Exception:
            print("Error, check if table isv_posts_json exists")

        posts = cur.fetchall()

        for p in posts:
            post = p[0]

            if not row_cnt:
                count = 0
                for key in post.keys():
                    ws.write(0, count, key)
                    count += 1

            row_cnt += 1
            count = 0
            for key in post.keys():
                if key == 'details' and len(post[key]) >= self._EXCEL_MAXIMUM_CELL:
                    post[key] = post[key][:self._EXCEL_MAXIMUM_CELL]

                if key == 'id' or key == 'author_id' or key == 'topic_id':
                    post[key] = '#' + str(post[key])

                ws.write(row_cnt, count, str(post[key]))
                count += 1
        cur.close()
        workbook.save(self._build_excel_filename('posts'))
        print("build post excel")

    def build_comments_excel_from_db(self):
        """
        build Excel report file from collected json files
        """
        row_cnt = 0
        workbook = xlwt.Workbook()
        ws = workbook.add_sheet("All comments")

        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select jdoc from isv_comments_json")
        except Exception:
            print("Error, check if table isv_comments_json exists")

        comments = cur.fetchall()

        for c in comments:
            comment = c[0]

            if not row_cnt:
                count = 0
                for key in comment.keys():
                    ws.write(0, count, key)
                    count += 1

            row_cnt += 1
            count = 0
            for key in comment.keys():
                if key == 'body' and len(comment[key]) >= self._EXCEL_MAXIMUM_CELL:
                    comment[key] = comment[key][:self._EXCEL_MAXIMUM_CELL]

                if key == 'id' or key == 'author_id' or key == 'post_id':
                    comment[key] = '#' + str(comment[key])

                ws.write(row_cnt, count, str(comment[key]))
                count += 1
        print("built comments excel")
        workbook.save(self._build_excel_filename('comments'))
        cur.close()

    def build_users_excel_from_db(self):
        """
        build Excel report file from collected json files
        """
        row_cnt = 0

        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select id, name, email, created_at, role from isv_users")
        except Exception:
            print("Error, check if table isv_users exists")
        workbook = xlwt.Workbook()
        ws = workbook.add_sheet("Users")
        users = cur.fetchall()
        for user in users:
            if not row_cnt:
                ws.write(0, 0, 'id')
                ws.write(0, 1, 'name')
                ws.write(0, 2, 'email')
                ws.write(0, 3, 'created_at')
                ws.write(0, 4, 'role')

            row_cnt += 1
            ws.write(row_cnt, 0, user[0])
            ws.write(row_cnt, 1, user[1])
            ws.write(row_cnt, 2, user[2])
            ws.write(row_cnt, 3, user[3])
            ws.write(row_cnt, 4, user[4])
        print("built users excel")
        workbook.save(self._build_excel_filename('user'))
        cur.close()

    def build_excel(self):
        workbook = xlwt.Workbook()
        ws1 = workbook.add_sheet("Posts")
        ws2 = workbook.add_sheet('Comments')
        ws3 = workbook.add_sheet('Users')



        self.build_posts_excel_from_db(ws1)
        self.build_comments_excel_from_db(ws2)
        self.build_users_excel_from_db(ws3)

        workbook.save(self._build_excel_filename('zendesk'))


    def _build_update_record(self):
        """
        record database update record
        :return:
        """
        cur = self._postgresql_conn.cursor()

        cur.execute("CREATE TABLE IF NOT EXISTS isv_update "
                    "(timestamp varchar PRIMARY KEY, date varchar, time varchar);")
        ts = time.time()
        t = time.localtime()
        year = str(t.tm_year)
        month = str(t.tm_mon)
        if len(month) < 2:
            month = '0' + month
        day = str(t.tm_mday)
        if len(day) < 2:
            day = '0' + day
        hour = str(t.tm_hour)
        if len(hour) < 2:
            hour = '0' + hour
        m = str(t.tm_min)
        if len(m) < 2:
            m = '0' + m
        sec = str(t.tm_sec)
        if len(sec) < 2:
            sec = '0' + sec
        date = '/'.join((year, month, day))
        tt = ':'.join((hour, m, sec))
        command = "INSERT INTO isv_update VALUES(%s,%s,%s);"
        cur.execute(command, (str(ts), date, tt))
        self._postgresql_conn.commit()
        cur.close()
        print("table isv_update updated")

    def update_isv_status(self, data):
        """
        update the isv_status column in isv_posts.
        :param data: a list contains each item is [post id, isv_status]
        :return:
        """

        cur = self._postgresql_conn.cursor()

        for record in data:
            if re.match('^[0-9]+', record[0]):
                command = "UPDATE isv_posts SET isv_status = %s WHERE id = %s"
                cur.execute(command, (str(record[1]), str(record[0])))

        self._postgresql_conn.commit()
        cur.close()
        print("isv_status in isv_posts updated")

    def get_isv_posts_data_for_processing(self):

        cur = self._postgresql_conn.cursor()

        command = "SELECT id,isv_status,topic_id,created_at_str FROM isv_posts"
        cur.execute(command)
        data = cur.fetchall()

        cur.close()
        return data

    def get_isv_topics_data(self, keys=['*']):
        cur = self._postgresql_conn.cursor()

        commend_key = ','.join(keys)
        command = "SELECT {0} FROM isv_topics".format(commend_key)

        try:
            cur.execute(command)
            data = cur.fetchall()
        except psycopg2.ProgrammingError as info:
            print(info)
            quit()

        cur.close()
        return data

    def get_isv_posts_data(self, keys=['*']):
        """
        fetch desire data from isv_posts.
        :param keys: a list of string contain column(s) want to fetch.
                        eg1. keys = ['*']  to fetch all data
                        eg2. keys = ['id', 'topic_id', 'isv_status'] to fetch these three columns
        :return: data fetched from database.
        """
        cur = self._postgresql_conn.cursor()

        commend_key = ','.join(keys)
        command = "SELECT {0} FROM isv_posts".format(commend_key)

        try:
            cur.execute(command)
            data = cur.fetchall()
        except psycopg2.ProgrammingError as info:
            print(info)
            quit()
        return data

    def get_isv_comments_data(self, keys=['*'], where=''):
        """
        fetch desire data from isv_comments.
        :param keys: a list of string contain column(s) want to fetch.
                        eg1. keys = ['*']  to fetch all data
                        eg2. keys = ['id', 'post_id', 'author_id'] to fetch these three columns
        :param where: contain where string
        :return: data fetched from database.
        # TODO: there is a problem what when running this function with parameter '*'.
        # Because there are too many comments and some comments contain image within detail,
        # so the result of this function may exceed some kind of limit,
        # so the return of this function may not be fully printable, thus only part of the result can be printed.
        """
        cur = self._postgresql_conn.cursor()

        commend_key = ','.join(keys)
        command = "SELECT {0} FROM isv_comments".format(commend_key)
        command += where

        try:
            cur.execute(command)
            data = cur.fetchall()
        except psycopg2.ProgrammingError as info:
            print(info)
            quit()

        return data

    def get_isv_users_data(self, keys=['*']):
        """
        fetch desire data from isv_users.
        :param keys: a list of string contain column(s) want to fetch.
                        eg1. keys = ['*']  to fetch all data
                        eg2. keys = ['id', 'role'] to fetch these columns
        :return: data fetched from database.
        """
        cur = self._postgresql_conn.cursor()

        commend_key = ','.join(keys)
        command = "SELECT {0} FROM isv_users".format(commend_key)

        try:
            cur.execute(command)
            data = cur.fetchall()
        except psycopg2.ProgrammingError as info:
            print(info)
            quit()
        return data

    def get_isv_support_data(self, keys=['*']):
        """
        fetch desire data from isv_support.
        :param keys: a list of string contain column(s) want to fetch.
                        eg1. keys = ['*']  to fetch all data
                        eg2. keys = ['id', 'name'] to fetch these columns
        :return: data fetched from database.
        """
        cur = self._postgresql_conn.cursor()

        commend_key = ','.join(keys)
        command = "SELECT {0} FROM isv_support".format(commend_key)

        try:
            cur.execute(command)
            data = cur.fetchall()
        except psycopg2.ProgrammingError as info:
            print(info)
            quit()
        return data



    def report_data(self):
        cur = self._postgresql_conn.cursor()

        # sql query for posts. Not using dynamic sql construction considering SQL INJECTION SECURITY CONCERN
        command = "SELECT id,html_url,title,topic_id,author_id,updated_at_str,isv_status from isv_posts "
        command += "WHERE isv_status in ('ExternalPending', 'InternalPending', 'Ongoingwork', 'PartnerPending', 'Open')"
        cur.execute(command)
        data_p = cur.fetchall()

        # build posts dict with post_id as key, fetched data as value
        posts = dict()
        for d in data_p:
            posts[d[0]] = list(d[1:])

        # selector for comments sql query
        post_id = ['\'' + i + '\'' for i in posts.keys()]
        selector = ','.join(post_id)
        selector = '(' + selector + ')'
        # print(selector)

        # sql query for comments
        command = "SELECT id,post_id,author_id,updated_at_str from isv_comments "
        command = ''.join([command, "WHERE post_id in ", selector])
        cur.execute(command)
        data_c = cur.fetchall()

        # sql query for supporters and topics
        data_s = self.get_isv_support_data()
        data_t = self.get_isv_topics_data()

        # topic dict
        topics = dict()
        for t in data_t:
            topics[t[0]] = list(t[1:])

        # supporters dict
        supporters = {}
        for s in data_s:
            supporters[s[0]] = s[1]

        # change data_c data structure from tuple to list
        data_c = [list(c) for c in data_c]

        # replace author_id in comments to supporter name if id is a supporter
        for comment in data_c:
            if comment[2] in supporters.keys():
                comment[2] = supporters[comment[2]]
            else:
                comment[2] = 'Partner'

        # build a post dict based on fetched data
        # key is post id
        # value is the latest response comment
        comments = dict()
        for comment in data_c:
            if comment[1] in comments.keys():
                if comments[comment[1]][3] < comment[3]:
                    comments[comment[1]] = comment
            else:
                comments[comment[1]] = comment

        res = []
        for key in comments.keys():
            d1 = datetime.datetime.strptime(comments[key][3][0:10], '%Y-%m-%d')
            d2 = datetime.datetime.today()
            day = (d2.date() - d1.date()).days
            topic = topics[posts[key][2]][2]
            last_res = comments[key][2]
            post_title = posts[key][1]
            post_status = posts[key][5]
            res.append([key, post_title, topic, post_status, day, last_res])
        print(res)
        return res

    def _build_json_tickets_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^ticket[0-9]+.json', file):
                    self._json_tickets_filename_list.append(os.path.join(self._save_path, file))

        # for j in self._json_comments_filename_list:
            # print(j)
            # with open(j, 'r', encoding='utf8') as f:
            #     data = f.read()
            #     print(data)

    def _parse_json_tickets_file(self):
        for file in self._json_tickets_filename_list:
            try:
                with open(file, 'r', encoding='utf8') as f:
                    data = json.load(f)
                    tickets = data['tickets']
                    for ticket in tickets:
                        self._tickets_id.append(str(ticket['id']))

            except json.JSONDecodeError:
                print("ERROR: Json file {0} decode error!".format(file))
                quit()

    def _initial_tickets_postgresql(self):
        """
        build ticket json table
        :return: None
        """
        self._build_json_tickets_file_list()
        # print('Tickets Json file names', self._json_tickets_filename_list)
        self._parse_json_tickets_file()
        #print('Tickets IDs', self._tickets_id)
        #print('Number of tickets', len(self._tickets_id))

        cur = self._postgresql_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS isv_tickets_json (id VARCHAR PRIMARY KEY, jdoc jsonb);")

        for filename in self._json_tickets_filename_list:
            data = self._load_json(filename)
            tickets = data['tickets']
            for ticket in tickets:
                cur.execute("SELECT * FROM isv_tickets_json WHERE id = %s;", (str(ticket['id']),))
                result = cur.fetchall()
                if result:
                    command = "UPDATE isv_tickets_json SET jdoc = %s WHERE id = %s;"
                    cur.execute(command, [psycopg2.extras.Json(ticket), str(ticket['id'])])
                else:
                    cur.execute("INSERT INTO isv_tickets_json (id, jdoc) VALUES(%s, %s);",
                                [str(ticket['id']), psycopg2.extras.Json(ticket)])

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_tickets_json updated")

    def _build_tickets_postgresql(self):
        """
        build tickets table
        :return: None
        """

        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select jdoc from isv_tickets_json")
        except Exception:
            print("Error, check if table isv_tickets_json exists")

        tickets = cur.fetchall()
        # tickets = [({})]
        # ticket are stored in dict, than encapsuled by fetchall() function, and put all ticket in a list
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS isv_tickets (
        id varchar PRIMARY KEY, 
        url varchar, 
        subject text, 
        status varchar, 
        created_at_timestamp real,
        created_at varchar, 
        updated_at_timestamp real,
        updated_at varchar,         
        submitter_id varchar,
        assignee_id varchar,
        problem_id varchar, 
        post_id varchar,
        classification varchar,
        sdk_type varchar
        );
        """
                    )

        for ticket in tickets:
            ticket = ticket[0]
            created_at = ticket['created_at']
            created_at_date = created_at[:10]
            created_at_time = created_at[11:-1]
            created_at = created_at_date + ' ' + created_at_time
            t = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at_timestamp = time.mktime(t)

            updated_at = ticket['updated_at']
            updated_at_date = updated_at[:10]
            updated_at_time = updated_at[11:-1]
            updated_at = updated_at_date + ' ' + updated_at_time
            t = time.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
            updated_at_timestamp = time.mktime(t)

            custom_fields = ticket['custom_fields']
            sdk_type = custom_fields[6]['value']
            post_id = custom_fields[11]['value']
            problem_id = custom_fields[20]['value']
            classification = custom_fields[22]['value']

            cur.execute("SELECT * FROM isv_tickets WHERE id = %s;", (str(ticket['id']),))
            result = cur.fetchall()
            if result:
                command = """UPDATE isv_tickets SET url = %s,
                                        subject = %s, 
                                        status = %s, 
                                        submitter_id = %s, 
                                        assignee_id = %s,
                                        problem_id = %s,
                                        created_at_timestamp = %s,
                                        created_at = %s, 
                                        updated_at_timestamp = %s,
                                        updated_at = %s, 
                                        post_id = %s,
                                        classification = %s,
                                        sdk_type = %s
                                        WHERE id = %s;
                                        """
                cur.execute(command, (
                    ticket['url'],
                    ticket['subject'],
                    ticket['status'],
                    str(ticket['submitter_id']),
                    str(ticket['assignee_id']),
                    problem_id,
                    created_at_timestamp,
                    ticket['created_at'],
                    updated_at_timestamp,
                    ticket['updated_at'],
                    str(post_id),
                    classification,
                    str(ticket['id']),
                    str(sdk_type)
                ))
            else:
                command = """
                                          INSERT INTO isv_tickets(
                                                    id, 
                                                    url, 
                                                    subject, 
                                                    status, 
                                                    submitter_id, 
                                                    assignee_id, 
                                                    problem_id, 
                                                    created_at_timestamp,
                                                    created_at, 
                                                    updated_at_timestamp,
                                                    updated_at,
                                                    post_id,
                                                    classification,
                                                    sdk_type
                                          )                           
                                          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                                          """
                cur.execute(command, (str(ticket['id']),
                                      ticket['url'],
                                      ticket['subject'],
                                      ticket['status'],
                                      str(ticket['submitter_id']),
                                      str(ticket['assignee_id']),
                                      problem_id,
                                      created_at_timestamp,
                                      ticket['created_at'],
                                      updated_at_timestamp,
                                      ticket['updated_at'],
                                      str(post_id),
                                      classification,
                                      str(sdk_type)
                                      ))
        self._postgresql_conn.commit()
        cur.close()
        print("table isv_tickets updated")

    def build_tickets_excel_from_db(self):
        """
        build Excel report file from tickets data
        """
        row_cnt = 0
        workbook = xlwt.Workbook()
        ws = workbook.add_sheet("Tickets")
        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select id,url,subject,status,created_at,updated_at,submitter_id,assignee_id,"
                        "problem_id,post_id,classification,sdk_type from isv_tickets")
        except Exception:
            print("Error, check if table isv_posts_json exists")

        tickets = cur.fetchall()
        keys = ['id', 'url', 'subject', 'status', 'created_at', 'updated_at',
                'submitter_id', 'assignee_id', 'problem_id', 'post_id', 'classification', 'sdk_type']
        for ticket in tickets:
            if not row_cnt:
                count = 0
                for key in keys:
                    ws.write(0, count, key)
                    count += 1

            row_cnt += 1
            count = 0
            for cnt in range(len(ticket)):
                data = ticket[cnt]
                # ticket[2] is subject, prevent longer than maximum
                if cnt == 2:
                    data = data[:self._EXCEL_MAXIMUM_CELL]

                #if cnt in [6, 7, 8, 9, 11] and ticket[cnt] is not None:
                #    data = str(data)

                ws.write(row_cnt, count, str(data))
                count += 1
        cur.close()
        workbook.save(self._build_excel_filename('tickets'))
        print("build ticket excel")

    def _build_json_tickets_comments_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^ticket_comm_.*.json', file):
                    self._json_tickets_comments_filename_list.append(os.path.join(self._save_path, file))

    def _initial_tickets_comments_postgresql(self):
        """
        build comment json table
        :return: None
        """
        self._build_json_tickets_comments_file_list()

        cur = self._postgresql_conn.cursor()
        cur.execute("""
                     CREATE TABLE IF NOT EXISTS isv_tcomments_json (
                     id VARCHAR PRIMARY KEY,
                     ticket_id VARCHAR,
                     jdoc jsonb);
                     """)

        for filename in self._json_tickets_comments_filename_list:
            data = self._load_json(filename)
            # find the ticket id from the filename
            # the filename is save_path + ticket_comments_ticketid.json
            save_path_length = len(self._save_path)
            ticket_id = filename[save_path_length+13:-5]

            comments = data['comments']
            for comment in comments:

                cur.execute("SELECT * FROM isv_tcomments_json WHERE id=%s;", (str(comment['id']),))
                result = cur.fetchall()

                if result:
                    command = "UPDATE isv_tcomments_json SET jdoc = %s WHERE id = %s;"
                    cur.execute(command, [psycopg2.extras.Json(comment), str(comment['id'])])
                else:
                    command = "INSERT INTO isv_tcomments_json (id, ticket_id, jdoc) VALUES(%s, %s, %s);"
                    cur.execute(command, [str(comment['id']), str(ticket_id), psycopg2.extras.Json(comment)])

        self._postgresql_conn.commit()
        cur.close()

        print("table isv_tickets_comments_json updated")

    def _build_tickets_comments_postgresql(self):
        """
        build tickets comments table
        :return: None
        """
        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select ticket_id,jdoc from isv_tcomments_json")
        except Exception:
            print("Error, check if table isv_comments_json exists")
        comments = cur.fetchall()
        # comments = [(ticket_id, {comment})]

        cur.execute("""
        CREATE TABLE IF NOT EXISTS isv_tcomments (
        id varchar PRIMARY KEY, 
        tickets_id varchar, 
        author_id varchar, 
        created_at_timestamp real,
        created_at varchar, 
        is_public bool
        );
        """)


        for comment in comments:
            ticket_id = comment[0]
            comment = comment[1]

            created_at = comment['created_at']
            created_at_date = created_at[:10]
            created_at_time = created_at[11:-1]
            created_at = created_at_date + ' ' + created_at_time
            t = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at_timestamp = time.mktime(t)

            cur.execute("SELECT * FROM isv_tcomments WHERE id=%s;", (str(comment['id']),))
            result = cur.fetchall()
            if result:
                command = """UPDATE isv_tcomments SET 
                                tickets_id = %s, 
                                author_id = %s, 
                                created_at_timestamp = %s,
                                created_at = %s, 
                                is_public = %s 
                                WHERE id = %s;
                            """
                cur.execute(command, (
                                      ticket_id,
                                      str(comment['author_id']),
                                      created_at_timestamp,
                                      comment['created_at'],
                                      comment['public'],
                                      str(comment['id']),
                ))
            else:
                command = "INSERT INTO isv_tcomments VALUES(%s,%s,%s,%s,%s,%s);"
                cur.execute(command, (str(comment['id']),
                                      ticket_id,
                                      str(comment['author_id']),
                                      created_at_timestamp,
                                      comment['created_at'],
                                      comment['public'],))

        self._postgresql_conn.commit()
        cur.close()
        print("table isv_tickets_comments updated")

    def build_tickets_comments_excel_from_db(self):
        """
        build Excel report file from tickets comments data
        """
        row_cnt = 0
        workbook = xlwt.Workbook()
        ws = workbook.add_sheet("Tickets Comments")
        cur = self._postgresql_conn.cursor()
        try:
            cur.execute("select id,tickets_id,author_id,created_at,is_public from isv_tcomments")
        except Exception:
            print("Error, check if table isv_tcomments_json exists")

        comments = cur.fetchall()
        keys = ['id', 'tickets_id', 'author_id', 'created_at', 'is_public']
        for comment in comments:
            if not row_cnt:
                count = 0
                for key in keys:
                    ws.write(0, count, key)
                    count += 1

            row_cnt += 1
            count = 0
            for cnt in range(len(comment)):
                data = comment[cnt]
                #if cnt in [6, 7, 8, 9, 11] and ticket[cnt] is not None:
                #    data = str(data)

                ws.write(row_cnt, count, str(data))
                count += 1
        cur.close()
        workbook.save(self._build_excel_filename('tickets_comments'))
        print("build ticket comments excel")

    def run_all(self):

        self._initial_posts_postgresql()
        self._build_posts_postgresql()
        self._initial_comments_postgresql()
        self._build_comments_postgresql()
        self._build_topics_postgresql()
        # self._build_users_postgresql()
        self._build_update_record()

        self.build_posts_excel_from_db()
        self.build_comments_excel_from_db()
        # self.build_users_excel_from_db()

        self._initial_tickets_postgresql()
        self._build_tickets_postgresql()
        self.build_tickets_excel_from_db()

        self._initial_tickets_comments_postgresql()
        self._build_tickets_comments_postgresql()
        self.build_tickets_comments_excel_from_db()

    def run_users(self):
        self._initial_posts_postgresql()
        self._build_users_postgresql()
        self.build_users_excel_from_db()

    def test(self):
        self._initial_tickets_postgresql()
        self._build_tickets_postgresql()
        self.build_tickets_excel_from_db()

        self._initial_tickets_comments_postgresql()
        self._build_tickets_comments_postgresql()
        self.build_tickets_comments_excel_from_db()