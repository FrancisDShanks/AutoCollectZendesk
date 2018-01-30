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
@Version: 0.3-Beta
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


# TO-DO: auto detect the total page count
class AutoZendesk(object):
    def __init__(self, username, passwd, chrome_driver_path,
                 postgresql_dbname, postgresql_user, postgresql_passwd, postgresql_host, postgresql_port):
        """
        initial method
        :param username: username of Zendesk JetAdvantage Support forum
        :param passwd: password of Zendesk JetAdvantage Support forum
        :param chrome_driver_path: path of chromedriver.exe tool, normally put in the same path as chrome.exe
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

        self._postgresql_dbname = postgresql_dbname
        self._postgresql_user = postgresql_user
        self._postgresql_passwd = postgresql_passwd
        self._postgresql_host = postgresql_host
        self._postgresql_port = postgresql_port
        self._postgresql_conn = None

        self._json_posts_filename_list = []
        self._json_comments_filename_list = []

    def _connect_postgresql(self):
        self._postgresql_conn = psycopg2.connect(dbname=self._postgresql_dbname,
                                                 user=self._postgresql_user,
                                                 password=self._postgresql_passwd,
                                                 host=self._postgresql_host,
                                                 port=self._postgresql_port)

    def _disconnect_postgresql(self):
        self._postgresql_conn.close()

    def drop_all_table_postgresql(self):
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS isv_posts;")
        cur.execute("DROP TABLE IF EXISTS isv_posts_json;")
        cur.execute("DROP TABLE IF EXISTS isv_comments;")
        cur.execute("DROP TABLE IF EXISTS isv_comments_json;")
        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()

    def initial_posts_postgresql(self):
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

    def initial_comments_postgresql(self):
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

    def build_posts_postgresql(self):
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
            print('-' * 20)
            post = p[0]
            created_at = post['created_at']
            created_at_date = created_at[:10]
            created_at_time = created_at[11:-1]
            created_at = created_at_date + ' ' + created_at_time
            t = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at_timestamp = time.mktime(t)
            print(created_at, created_at_timestamp)

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

    def build_comments_postgresql(self):
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        # TO-DO exception handler when table not exists
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

    @staticmethod
    def _load_json(filename):
        """
        load json file
        :param filename: file name of json file need to load
        :return: raw data loaded from json file
        """
        with open(filename, encoding='utf8') as json_file:
            data = json.load(json_file)
        return data

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

    def _remove_json_comments_files(self):
        """
        remove generated json file(s)
        """
        for full_path in self._json_comments_filename_list:
            if os.path.exists(full_path):
                os.remove(full_path)

    def build_posts_excel(self):
        """
        build Excel report file from collected json files
        TODO : json file name already in list, no need to build filename again
        """
        local_time = time.strftime("_%Y_%m_%d", time.localtime(time.time()))
        save_filename = self._save_path + 'Posts' + local_time + '.xls'
        if os.path.exists(save_filename):
            os.remove(save_filename)
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
        wb.save(save_filename)

    def build_comments_excel(self):
        """
        build Excel report file from collected json files
        """
        local_time = time.strftime("_%Y_%m_%d", time.localtime(time.time()))
        save_filename = self._save_path + 'comments' + local_time + '.xls'
        if os.path.exists(save_filename):
            os.remove(save_filename)
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
        wb.save(save_filename)

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

    def _collect_posts(self):
        """
        collect json file(s) from Zendesk API
        """
        for page_cnt in range(1, self._total_page + 1):
            js = 'window.open("https://jetadvantage.zendesk.com//api/v2/community/posts.json?page=' + str(
                page_cnt) + '");'
            print(js)
            self._browser.execute_script(js)
            base_handler = self._browser.current_window_handle
            all_handler = self._browser.window_handles
            for handler in all_handler:
                if handler != base_handler:
                    self._browser.switch_to.window(handler)

                    file_name = 'post' + str(page_cnt) + '.json'
                    full_path = self._save_path + file_name
                    if os.path.exists(full_path):
                        os.remove(full_path)
                    self._json_posts_filename_list.append(full_path)
                    file_object = codecs.open(full_path, 'w', 'utf-8')
                    raw_data = self._browser.page_source

                    file_object.write(self._remove_html_tags(raw_data))
                    file_object.close()
                    self._browser.close()
            self._browser.switch_to.window(base_handler)

    def collect_posts_only(self):
        self._login_zendesk()
        self._collect_posts()
        self._logout_zendesk()

    def collect_comments_only(self):
        self._login_zendesk()
        self._collect_comments()
        self._logout_zendesk()

    def collect_posts_and_comments(self):
        self._login_zendesk()
        self._collect_posts()
        self.initial_posts_postgresql()
        self.build_posts_postgresql()
        self._collect_comments()
        self.initial_comments_postgresql()
        self._logout_zendesk()
        self.build_comments_postgresql()
        self._remove_json_posts_files()
        self._remove_json_comments_files()

    def _collect_comments(self):
        """
        collect comments
        :return: None
        """
        # TO-DO : now I consider all comments only have one page, should detect page count
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        cur.execute("select id from isv_posts")
        # data structure : [(id)(id),...,(id)]
        load_ids = cur.fetchall()
        ids = [id0[0] for id0 in load_ids]
        self._postgresql_conn.commit()
        cur.close()
        self._disconnect_postgresql()

        # https://jetadvantage.zendesk.com/api/v2/community/posts/220794928/comments.json
        for id0 in ids:
            js = 'window.open("https://jetadvantage.zendesk.com/api/v2/community/posts/' + id0 + '/comments.json");'
            print(js)
            self._browser.execute_script(js)
            base_handler = self._browser.current_window_handle
            all_handler = self._browser.window_handles
            for handler in all_handler:
                if handler != base_handler:
                    self._browser.switch_to.window(handler)

                    file_name = 'comment_' + id0 + '.json'
                    full_path = self._save_path + file_name
                    if os.path.exists(full_path):
                        os.remove(full_path)
                    self._json_comments_filename_list.append(full_path)
                    file_object = codecs.open(full_path, 'w', 'utf-8')
                    raw_data = self._browser.page_source

                    file_object.write(self._remove_html_tags(raw_data))
                    file_object.close()
                    self._browser.close()
            self._browser.switch_to.window(base_handler)
