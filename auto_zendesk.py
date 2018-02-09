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
@Version: 	02/2018 0.5-Beta: separate crawling logic and database logic 
			02/2018 0.4-Beta: add database update recording
			02/2018 0.3-Beta: add users and topics data collecting
			01/2018 0.2-Beta: add database storage
			01/2018 0.1-Beta: build zendesk auto collect function
			
"""

# core mods
import codecs
import os
import json
import xlwt
import time
import re

# 3rd party mods
import psycopg2
import psycopg2.extras
from selenium import webdriver


# TODO: update database instead of rebuild it
class AutoZendesk(object):
    def __init__(self, username, passwd, chrome_driver_path,
                 postgresql_dbname, postgresql_user, postgresql_passwd, postgresql_host, postgresql_port):
        """
        initial method
        :param username: username of Zendesk JetAdvantage Support forum
        :param passwd: password of Zendesk JetAdvantage Support forum
        :param chrome_driver_path: path of chromedriver.exe tool, normally put in the same path as chrome.exe
        :param postgresql_dbname: database name
        :param postgresql_user: database user name
        :param postgresql_passwd: passwd for the user
        :param postgresql_host: database host
        :param postgresql_port: database port
        """
        self._username = username
        self._passwd = passwd
        self._chrome_driver_path = chrome_driver_path
        self._save_path = os.path.abspath('.') + '\\'

        self._total_page = 9
        self._initial_page = r'https://developers.hp.com/user/login?destination=hp-zendesk-sso'
        self._browser = None
        self._EXCEL_MAXIMUM_CELL = 32767
        self._zendesk_main_page = r'https://jetadvantage.zendesk.com/hc/en-us'
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
        cur.execute("DROP TABLE IF EXISTS isv_posts_json;")
        cur.execute("DROP TABLE IF EXISTS isv_comments;")
        cur.execute("DROP TABLE IF EXISTS isv_comments_json;")
        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()
        print("Dropped tables")

    def _initial_posts_postgresql(self):
        """
        build post json table
        :return: None
        """
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS isv_posts_json (jdoc jsonb);")

        for filename in self._json_posts_filename_list:
            data = self._load_json(filename)
            posts = data['posts']
            for post in posts:
                cur.execute("INSERT INTO isv_posts_json (jdoc) VALUES(%s);", [psycopg2.extras.Json(post)])

        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()
        print("table isv_posts_json built")

    def _get_page_count(self):
        file = self._save_path + 'post1.json'
        try:
            with open(file, 'r', encoding='utf8') as f:
                data = json.load(f)
                return data['page_count']
        except IOError:
            print("ERROR: IO ERROR when load {0}".format(file))
            quit()
        except json.JSONDecodeError:
            print("ERROR: Json file {0} decode error!".format(file))
            quit()

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
                if file[:3] == 'pos':
                    self._json_posts_filename_list.append(self._save_path + file)

    def _build_json_users_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if file[:3] == 'use':
                    self._json_users_filename_list.append(self._save_path + file)

    def _build_json_comments_file_list(self):
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if file[:3] == 'com':
                    self._json_comments_filename_list.append(self._save_path + file)

        for j in self._json_comments_filename_list:
            print(j)
            with open(j, 'r', encoding='utf8') as f:
                data = f.read()
                print(data)

    def _initial_comments_postgresql(self):
        """
        build comment json table
        :return: None
        """
        self._build_json_comments_file_list()
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS isv_comments_json (jdoc jsonb);")

        for filename in self._json_comments_filename_list:
            data = self._load_json(filename)
            comments = data['comments']
            for comment in comments:
                cur.execute("INSERT INTO isv_comments_json (jdoc) VALUES(%s);", [psycopg2.extras.Json(comment)])

        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()
        print("table isv_comments_json built")

    def _build_topics_postgresql(self):
        """

        :return:
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
        cur.execute("DROP TABLE IF EXISTS isv_topics")
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
        cur.execute("DROP TABLE IF EXISTS isv_users")
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
        cur.execute("select * from isv_comments_json")
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

    @staticmethod
    def _remove_html_tags(raw):
        """
        remove HTML tags from raw data
        :param raw: raw data need to remove html tags
        :return: data with html tags removed
        """
        dr = re.compile(r'<[^>]+>', re.S)
        dd = dr.sub('', raw)
        return dd

    def _remove_json_posts_files(self):
        """
        remove generated json file(s)
        """
        for page_count in range(1, self._total_page + 1):
            file_name = 'post' + str(page_count) + '.json'
            full_path = self._save_path + file_name
            if os.path.exists(full_path):
                os.remove(full_path)
        print("removing posts json files ...")

    def _remove_json_comments_files(self):
        """
        remove generated json file(s)
        """
        for full_path in self._json_comments_filename_list:
            if os.path.exists(full_path):
                os.remove(full_path)
        print("removing comments json files ...")

    def _remove_json_users_topics_files(self):
        """
        remove generated json file(s)
        """
        for full_path in self._json_users_filename_list:
            if os.path.exists(full_path):
                os.remove(full_path)
        if os.path.exists(self._save_path + 'topics.json'):
            os.remove(self._save_path + 'topics.json')

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
        cur.execute("select * from isv_posts_json")
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
        cur.execute("select * from isv_comments_json")
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

    def _login_zendesk(self):
        """
        open Zendesk and login
        :return: None
        """
        # Optional argument, if not specified will search path.
        self._browser = webdriver.Chrome(self._chrome_driver_path)
        self._browser.get(self._initial_page)
        search_box = self._browser.find_element_by_name('name')
        search_box.send_keys(self._username)
        search_box = self._browser.find_element_by_name('pass')
        search_box.send_keys(self._passwd)
        search_box.submit()
        self._browser.get(self._zendesk_main_page)
        time.sleep(15)

    def _logout_zendesk(self):
        """
        close and logout Zendesk
        :return: None
        """
        self._browser.quit()

    def _collect_browser_page(self, js, file_name):
        """
        open a browser page and collect it
        :param js: javascript command to open new browser page
        :param file_name: file name to save collected data
        :return: None
        """
        self._browser.execute_script(js)
        base_handler = self._browser.current_window_handle
        all_handler = self._browser.window_handles
        for handler in all_handler:
            if handler != base_handler:
                self._browser.switch_to.window(handler)
                full_path = self._save_path + file_name
                if os.path.exists(full_path):
                    os.remove(full_path)
                file_object = codecs.open(full_path, 'w', 'utf-8')
                raw_data = self._browser.page_source

                file_object.write(self._remove_html_tags(raw_data))
                file_object.close()
                self._browser.close()
        self._browser.switch_to.window(base_handler)

    def _collect_posts(self):
        """
        collect json file(s) from Zendesk API
        """
        # collect the first page to get total page count
        js = 'window.open("https://jetadvantage.zendesk.com/api/v2/community/posts.json?page=1");'
        file_name = 'post1.json'
        self._collect_browser_page(js, file_name)
        # collect the rest pages
        for page_cnt in range(2, self._total_page + 1):
            js = 'window.open("https://jetadvantage.zendesk.com/api/v2/community/posts.json?page=' + str(
                page_cnt) + '");'
            file_name = 'post' + str(page_cnt) + '.json'
            self._collect_browser_page(js, file_name)

    def _collect_comments(self):
        """
        collect comments
        :return: None
        """
        # TODO : now I consider all comments only have one page, should detect page count
        # https://jetadvantage.zendesk.com/api/v2/community/posts/220794928/comments.json
        self._build_json_posts_file_list()
        self._parse_json_posts_file()
        for id0 in self._posts_id:
            js = 'window.open("https://jetadvantage.zendesk.com/api/v2/community/posts/' + id0 + '/comments.json");'
            file_name = 'comments_' + id0 + '.json'
            self._collect_browser_page(js, file_name)

    def _collect_users(self):
        """
        collect zendesk forum user info
        :return: None
        """
        # TODO: auto build query http address
        print("Collecting Users...")
        # https://jetadvantage.zendesk.com/api/v2/search.json?page=1&query=created%3C2018-12-30
        for page_cnt in range(1, 3):
            js = 'window.open("https://jetadvantage.zendesk.com/api/v2/search.json?page=' + str(page_cnt) + \
                 '&query=created%3C2018-12-30");'
            file_name = 'users_' + str(page_cnt) + '.json'
            self._collect_browser_page(js, file_name)

    def _collect_topics(self):
        """
        collect zendesk forum topics info
        :return: None
        """
        print("Collecting Topics...")
        # https://jetadvantage.zendesk.com/api/v2/community/topics.json
        js = 'window.open("https://jetadvantage.zendesk.com/api/v2/community/topics.json");'
        file_name = 'topics.json'
        self._collect_browser_page(js, file_name)

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
        self._drop_all_table_postgresql()
        self._login_zendesk()
        self._collect_posts()
        self._collect_comments()
        self._collect_users()
        self._collect_topics()
        self._logout_zendesk()

        self._initial_posts_postgresql()
        self._build_posts_postgresql()
        self._initial_comments_postgresql()
        self._build_comments_postgresql()
        self.build_posts_excel()
        self.build_comments_excel()
        self._build_topics_postgresql()
        self._build_users_postgresql()
        self._build_update_record()
        self._remove_json_posts_files()
        self._remove_json_comments_files()
        self._remove_json_users_topics_files()
