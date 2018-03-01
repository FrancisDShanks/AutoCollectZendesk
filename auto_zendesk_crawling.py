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
import time
import re

from selenium import webdriver


# TODO: update database instead of rebuild it
class AutoZendeskCrawling(object):
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
        self._zendesk_main_page = r'https://jetadvantage.zendesk.com/hc/en-us'

        self._posts_id = []
        self._json_posts_filename_list = []

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
        # find total page count from the first page
        self._total_page = self._get_page_count()
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

    def run_all(self):
        self._login_zendesk()

        self._collect_posts()
        self._collect_comments()
        self._collect_users()
        self._collect_topics()

        self._logout_zendesk()
