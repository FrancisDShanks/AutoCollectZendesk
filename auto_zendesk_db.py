# -*- coding: utf-8 -*-
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
@Version: 	03/2018 0.6-Beta:   1. update the tool to only collect the necessary data
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
import re

# 3rd party mods
import psycopg2
import psycopg2.extras


# TODO: update database instead of rebuild it
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
        self._save_path = os.path.abspath('.') + '\\'

        self._EXCEL_MAXIMUM_CELL = 32767
        self._posts_id = []

        self._postgresql_dbname = postgresql_dbname
        self._postgresql_user = postgresql_user
        self._postgresql_passwd = postgresql_passwd
        self._postgresql_host = postgresql_host
        self._postgresql_port = postgresql_port
        self._postgresql_conn = None

        self._json_posts_filename_list = []
        self._json_comments_filename_list = []
        self._json_users_filename_list = []

    def _connect_postgresql(self):
        """
        connect postgresql database
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
        disconnect database
        :return: None
        """
        self._postgresql_conn.close()
        print("Disconnect database")

    def _drop_all_table_postgresql(self):
        """
        drop all posts and comments tables in database
        :return: None
        """
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS isv_posts;")

        cur.execute("DROP TABLE IF EXISTS isv_comments;")
        cur.execute("DROP TABLE IF EXISTS isv_comments_json;")
        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()

    def _initial_posts_postgresql(self):
        """
        build post json table
        :return: None
        """
        self._build_json_posts_file_list()
        self._parse_json_posts_file()

        self._connect_postgresql()
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
        self._disconnect_postgresql()
        # print("table isv_posts_json built")

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
                    self._json_posts_filename_list.append(self._save_path + file)

    def _build_json_users_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^user.*.json', file):
                    self._json_users_filename_list.append(self._save_path + file)

    def _build_json_comments_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^comment.*.json', file):
                    self._json_comments_filename_list.append(self._save_path + file)

        # for j in self._json_comments_filename_list:
            # print(j)
            # with open(j, 'r', encoding='utf8') as f:
            #     data = f.read()
            #     print(data)

    def _initial_comments_postgresql(self):
        """
        build comment json table
        :return: None
        """
        self._build_json_comments_file_list()
        self._connect_postgresql()
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
        self._disconnect_postgresql()
        print("table isv_comments_json built")

    def _build_topics_postgresql(self):

        """
        build isv_topics table in database
        :return: None
        """
        # TODO:check if json file exists
        self._connect_postgresql()
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

        data = self._load_json(self._save_path + r'topics.json')
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
        self._disconnect_postgresql()

    def _build_users_postgresql(self):
        """

        :return:
        """
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        self._build_json_users_file_list()
        """
        "id": varchar
        "url": varchar
        "name": varchar
        "email": varchar
        "created_at": varchar
        "updated_at": varchar
        "time_zone": varchar
        "phone": varchar
        "shared_phone_number": varchar
        "photo": text
        "locale_id": varchar
        "locale": varchar
        "organization_id": varchar
        "role": varchar
        "verified": bool
        "result_type": varchar
        """
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
        result_type varchar       
        );
        """
                    )

        for filename in self._json_users_filename_list:
            data = self._load_json(filename)
            users = data['results']
            for user in users:
                # TODO: why 3 tickets here?
                if 'via' in user.keys():
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

                if result:
                    command = """UPDATE isv_users SET url = %s,
                                name = %s,
                                email = %s,
                                created_at = %s,
                                updated_at = %s,
                                time_zone = %s,
                                phone = %s,
                                shared_phone_number = %s,
                                photo = %s,
                                locale_id = %s,
                                locale = %s,
                                organization_id = %s,
                                role = %s,
                                verified = %s,
                                result_type = %s WHERE id = %s   
                                """
                    cur.execute(command, (
                        user['url'],
                        user['name'],
                        user['email'],
                        created_at,
                        updated_at,
                        user['time_zone'],
                        str(user['phone']),
                        str(user['shared_phone_number']),
                        str(user['photo']),
                        str(user['locale_id']),
                        user['locale'],
                        str(user['organization_id']),
                        user['role'],
                        user['verified'],
                        user['result_type'],
                        str(user['id'])
                    ))
                else:
                    command = "INSERT INTO isv_users VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    cur.execute(command, (str(user['id']),
                                          user['url'],
                                          user['name'],
                                          user['email'],
                                          created_at,
                                          updated_at,
                                          user['time_zone'],
                                          str(user['phone']),
                                          str(user['shared_phone_number']),
                                          str(user['photo']),
                                          str(user['locale_id']),
                                          user['locale'],
                                          str(user['organization_id']),
                                          user['role'],
                                          user['verified'],
                                          user['result_type']
                                          ))

        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()

    def _build_posts_postgresql(self):
        """
        build post table
        :return: None
        """
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        # TODO exception handler when table not exists
        cur.execute("select jdoc from isv_posts_json")
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

        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()
        print("table isv_posts built")

    def _build_comments_postgresql(self):
        """
        build comment table
        :return: None
        """
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        # TODO exception handler when table not exists
        cur.execute("select jdoc from isv_comments_json")
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
        self._disconnect_postgresql()
        print("table isv_comments built")

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
        :param data_type: 'posts' or 'comments'
        :return: excel filename
        """
        local_time = time.strftime("_%Y_%m_%d", time.localtime(time.time()))
        save_filename = self._save_path + data_type + local_time + '.xls'
        if os.path.exists(save_filename):
            os.remove(save_filename)

        return save_filename

    def build_posts_excel(self):
        """
        build Excel report file from collected json files
        """
        wb = xlwt.Workbook()
        ws = wb.add_sheet('All Posts')
        row_cnt = 0

        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        # TODO exception handler when table not exists
        cur.execute("select jdoc from isv_posts_json")
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
        wb.save(self._build_excel_filename('posts'))

    def build_comments_excel(self):
        """
        build Excel report file from collected json files
        """
        wb = xlwt.Workbook()
        ws = wb.add_sheet('All comments')
        row_cnt = 0

        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        # TODO exception handler when table not exists
        cur.execute("select jdoc from isv_comments_json")
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
        wb.save(self._build_excel_filename('comments'))

    def _build_update_record(self):
        """
        record database update record
        :return:
        """
        self._connect_postgresql()
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
        m = str(t.tm_min)
        sec = str(t.tm_sec)
        date = '/'.join((year, month, day))
        tt = ':'.join((hour, m, sec))
        command = "INSERT INTO isv_update VALUES(%s,%s,%s);"
        cur.execute(command, (str(ts), date, tt))
        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()

    def run_all(self):
        self._initial_posts_postgresql()
        self._build_posts_postgresql()
        self._initial_comments_postgresql()
        self._build_comments_postgresql()

        self.build_posts_excel()
        self.build_comments_excel()

        self._build_topics_postgresql()
        self._build_users_postgresql()
        self._build_update_record()

    def test(self):
        self._build_json_users_file_list()
