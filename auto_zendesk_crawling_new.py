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
import codecs
import os
import json
import time
import datetime
import re
import requests

# 3rd party mods
import configure

class AutoZendeskCrawling(object):
    def __init__(self, username, passwd):
        """
        Collect data(posts, comments, users, topics) from zendesk forum.
        :param username: username of Zendesk JetAdvantage Support forum
        :param passwd: password of Zendesk JetAdvantage Support forum
        :param chrome_driver_path: path of chromedriver.exe tool, normally put in the same path as chrome.exe

        """
        self._token = "Bearer 10190a3ab2d7803e9f34f5b278d629eb97ab22cd2ab29366433dc9c56a4c1431"
        self._header = {'Authorization' : self._token}
        self._username = username
        self._passwd = passwd
        self._save_path = configure.OUTPUT_PATH

        # total page of posts
        # set to 1 when initial and it will dynamically updated running post collection function
        self._total_page = 1
        self._zendesk_main_page = r'https://jetadvantage.zendesk.com/hc/en-us'

        # this parameter determine how may days(latest days) of data to collect
        self._LATEST_DAYS_DATA_TO_COLLECT = 5

        # sleep some seconds after logged in zendesk to wait the page full loaded to browswe
        # when having a bad network connection
        self._SLEEP_AFTER_LOG_IN = 5

        self._posts_id = []
        self._json_posts_filename_list = []

    def _get_page_count(self):
        """
        update the right total page count based on the first collected post json file.
        :return: None
        """
        file = os.path.join(self._save_path, 'post1.json')
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
        """
        parse posts json files, load post's comments records for comments collection function.
        only load post witch is updated in self._LATEST_DAYS_DATA_TO_COLLECT days.
        :return: None
        """
        for file in self._json_posts_filename_list:
            try:
                with open(file, 'r', encoding='utf8') as f:
                    data = json.load(f)
                    posts = data['posts']
                    for post in posts:
                        update_str = post['updated_at']
                        update_time = datetime.datetime.strptime(update_str, "%Y-%m-%dT%H:%M:%SZ")

                        c_time = datetime.datetime.now()
                        days = (c_time - update_time).days

                        # only collects those posts' comments which has been updated in n days
                        if days < self._LATEST_DAYS_DATA_TO_COLLECT:
                            # print(post['id'], update_time)
                            # print(days)
                            self._posts_id.append(str(post['id']))
            except IOError:
                print("ERROR: IO ERROR when load {0}".format(file))
                quit()
            except json.JSONDecodeError:
                print("ERROR: Json file {0} decode error!".format(file))
                quit()

    def _build_json_posts_file_list(self):
        """
        build posts json file list.
        :return: None
        """
        for root, dirs, files in os.walk(self._save_path):
            for file in files:
                if re.match('^post.*.json', file):
                    self._json_posts_filename_list.append(os.path.join(self._save_path, file))

    @staticmethod
    def _remove_html_tags(raw):
        """
        remove HTML tags from raw data.
        :param raw: raw data need to remove the html tags.
        :return: data with html tags removed.
        """
        dr = re.compile(r'<[^>]+>', re.S)
        dd = dr.sub('', raw)
        return dd

    def _collect_data_from_api(self, url, file_name):
        """
        open a browser page and collect it.
        :param js: javascript command to open new browser page
        :param file_name: file name to save collected data
        :return: None
        """
        full_path = os.path.join(self._save_path, file_name)
        try:
            if os.path.exists(full_path):
                os.remove(full_path)
            file_object = codecs.open(full_path, 'w', 'utf-8')
            r = requests.get(url, headers=self._header)
            raw_data = r.json()

            file_object.write(json.dumps(raw_data))
            file_object.close()
        except IOError:
            print("ERROR: IO ERROR when save {0}".format(full_path))
            quit()
        except OSError:
            print("ERROR: OS ERROR when save {0}".format(full_path))
            quit()


    def _collect_posts(self):
        """
        collect json file(s) from Zendesk API.
        """
        # collect the first page to get total page count
        url = 'https://jetadvantage.zendesk.com/api/v2/community/posts.json?page=1'
        file_name = 'post1.json'
        self._collect_data_from_api(url, file_name)

        # find total page count from the first page
        self._total_page = self._get_page_count()
        # collect the rest pages
        for page_cnt in range(2, self._total_page + 1):
            url = 'https://jetadvantage.zendesk.com/api/v2/community/posts.json?page=' + str(
                page_cnt)
            file_name = 'post' + str(page_cnt) + '.json'
            self._collect_data_from_api(url, file_name)

    def _collect_comments(self):
        """
        collect comments.
        Only collect comments belong to post updated/created in recent particular days.
        :return: None
        """
        # TODO : now I consider all comments only have one page, should detect page count
        # comments query format
        # https://jetadvantage.zendesk.com/api/v2/community/posts/220794928/comments.json
        self._build_json_posts_file_list()
        self._parse_json_posts_file()
        for id0 in self._posts_id:
            url = 'https://jetadvantage.zendesk.com/api/v2/community/posts/' + id0 + '/comments.json'
            file_name = 'comments_' + id0 + '.json'
            self._collect_data_from_api(url, file_name)

    def _collect_users(self):
        """
        collect zendesk forum user info.
        :return: None
        """
        # TODO: 1.auto detect page count. 2.auto build query http address.
        # print("Collecting Users...")
        # https://jetadvantage.zendesk.com/api/v2/search.json?page=1&query=created%3C2018-12-30
        for page_cnt in range(1, 3):
            url = 'https://jetadvantage.zendesk.com/api/v2/search.json?page=' + str(page_cnt) + \
                 '&query=created%3C2018-12-30'
            file_name = 'users_' + str(page_cnt) + '.json'
            self._collect_data_from_api(url, file_name)

    def _collect_topics(self):
        """
        collect zendesk forum topics info.
        :return: None
        """
        # print("Collecting Topics...")
        # https://jetadvantage.zendesk.com/api/v2/community/topics.json
        url = 'https://jetadvantage.zendesk.com/api/v2/community/topics.json'
        file_name = 'topics.json'
        self._collect_data_from_api(url, file_name)

    def run_all(self):
        self._collect_posts()
        self._collect_comments()
        self._collect_users()
        self._collect_topics()
