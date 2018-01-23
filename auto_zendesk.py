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
@Version: 0.2-Beta
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
    def __init__(self, username, passwd, chrome_driver_path):
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

        self._postgresql_database = "isv_zendesk"
        self._postgresql_user = "postgres"
        self._postgresql_passwd = "Dxf3529!"
        self._postgresql_host = "127.0.0.1"
        self._postgresql_port = "5432"
        self._postgresql_conn = None

        self._json_filename_list = []

    def _connect_postgresql(self):
        self._postgresql_conn = psycopg2.connect(dbname=self._postgresql_database,
                                                 user=self._postgresql_user,
                                                 password=self._postgresql_passwd,
                                                 host=self._postgresql_host,
                                                 port=self._postgresql_port)

    def initial_postgresql(self):
        self._connect_postgresql()
        cur = self._postgresql_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS isv_posts_json (jdoc jsonb);")

        for filename in self._json_filename_list:
            print(filename)
            data = self._load_json(filename)
            posts = data['posts']
            for post in posts:
                print(type(post), post)
                cur.execute("INSERT INTO isv_posts_json (jdoc) VALUES(%s);", [psycopg2.extras.Json(post)])

        self._postgresql_conn.commit()
        cur.close()
        self._postgresql_conn.close()

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

    def _remove_json_files(self):
        """
        remove generated json file(s)
        """
        for page_count in range(1, self._total_page + 1):
            file_name = 'post' + str(page_count) + '.json'
            full_path = self._save_path + file_name
            if os.path.exists(full_path):
                os.remove(full_path)

    def build_excel(self):
        """
        build Excel report file from collected json files
        """
        local_time = time.strftime("_%Y_%m_%d", time.localtime(time.time()))
        save_filename = self._save_path + 'Posts' + local_time + '.xls'
        if os.path.exists(save_filename):
            os.remove(save_filename)
        wb = xlwt.Workbook()
        ws = wb.add_sheet('All Posts')
        row_cnt = 0
        for i in range(1, self._total_page + 1):
            filename = self._save_path + 'post' + str(i) + '.json'
            data = self._load_json(filename)
            posts = data['posts']

            if not row_cnt:
                count = 0
                for key in posts[0].keys():
                    ws.write(0, count, key)
                    count += 1

            for post in posts:
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
        self._remove_json_files()

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

    def collect_posts(self):
        """
        collect json file(s) from Zendesk API
        """
        self._login_zendesk()
        for page_cnt in range(1, self._total_page + 1):
            js = 'window.open("https://jetadvantage.zendesk.com//api/v2/community/posts.json?page=' + str(
                page_cnt) + '");'
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
                    self._json_filename_list.append(full_path)
                    file_object = codecs.open(full_path, 'w', 'utf-8')
                    raw_data = self._browser.page_source

                    file_object.write(self._remove_html_tags(raw_data))
                    file_object.close()
                    self._browser.close()
            self._browser.switch_to.window(base_handler)
        self._logout_zendesk()
