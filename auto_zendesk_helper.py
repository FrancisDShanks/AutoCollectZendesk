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
import time
import os
import re
import shutil


class AutoZendeskHelper(object):
    def __init__(self):
        """
        initial method
        """
        self._save_path = os.path.abspath('.') + '\\'

        self._json_posts_filename_list = []
        self._json_comments_filename_list = []
        self._json_users_filename_list = []

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

    def _remove_json_posts_files(self):
        """
        remove generated json file(s)
        """
        for file in self._json_posts_filename_list:
            if os.path.exists(file):
                os.remove(file)
        print("removing posts json files ...")

    def _remove_json_comments_files(self):
        """
        remove generated json file(s)
        """
        for file in self._json_comments_filename_list:
            if os.path.exists(file):
                os.remove(file)
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

    def remove_all_json_files(self):
        for r in os.listdir(self._save_path):
            if os.path.isfile(r):
                if re.match('^post.*.json', r) or re.match('^comment.*.json', r) or\
                        re.match('^user.*.json', r) or re.match('^topic.*.json', r):
                    os.remove(r)


    @staticmethod
    def move_excel():
        source = 'D:\\workspace_Francis_Du\\PycharmProjects\\zendesk\\'
        des1 = 'D:\\workspace_Francis_Du\\PycharmProjects\\mysite\\static\\docs\\'
        des2 = '\\\\192.168.8.55\\ISV-Share\\FrancisDu\\zendeskRecords\\'

        t = time.localtime()
        year = str(t.tm_year)
        month = str(t.tm_mon)
        if len(month) < 2:
            month = '0' + month
        day = str(t.tm_mday)
        if len(day) < 2:
            day = '0' + day

        post_name = '_'.join((str(year), str(month), str(day)))
        post_name = ''.join(('posts_', post_name, '.xls'))
        com_name = '_'.join((str(year), str(month), str(day)))
        com_name = ''.join(('comments_', com_name, '.xls'))

        shutil.copyfile(source + post_name, des1 + post_name)
        shutil.copyfile(source + post_name, des2 + post_name)
        shutil.copyfile(source + com_name, des1 + com_name)
        shutil.copyfile(source + com_name, des2 + com_name)
        os.remove(source + post_name)
        os.remove(source + com_name)

    def run_remove_json_files(self):
        self._build_json_posts_file_list()
        self._build_json_comments_file_list()
        self._build_json_users_file_list()

        self._remove_json_posts_files()
        self._remove_json_comments_files()
        self._remove_json_users_topics_files()
