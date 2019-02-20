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

import re

import matplotlib.pyplot as plt
import numpy
import pygal

import src.auto_zendesk_db as auto_zendesk_db
import src.auto_zendesk_helper as auto_zendesk_helper
import os
import configure as configure


def pie_chart_pygal():
    h = auto_zendesk_helper.AutoZendeskHelper()
    data = h.read_xlsx()
    data_classification = dict()
    total = 0
    for line in data:
        if not re.match('^\d+', line[0]):
            continue

        if line[1] in data_classification.keys():
            data_classification[line[1]] += 1
        else:
            data_classification[line[1]] = 1
        total += 1
    print(data_classification)

    pie_chart = pygal.Pie()
    pie_chart.title = 'ISV SUPPORT JAVA/.NET Post Distribution'

    for d in data_classification.keys():
        percent = data_classification[d] / total
        percent = "%.2f%%" % (percent * 100)
        pie_chart.add(''.join((d, '-', percent)), data_classification[d])

    pie_chart.render_to_file(os.path.join(configure.OUTPUT_PATH, 'pie_chart.svg'))


def pie_chart_pyplot():
    d = auto_zendesk_db.AutoZendeskDB("isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")
    post_data = d.get_isv_posts_data(['id', 'isv_status'])

    data_classes = {}
    total = 0
    for data in post_data:
        if not data[1]:
            continue
        if data[1] not in data_classes.keys():
            data_classes[data[1]] = 1
        else:
            data_classes[data[1]] += 1
        total += 1

    labels = []
    values = []
    explode = []
    for key in data_classes.keys():
        labels.append(key + ' -' + str(data_classes[key]))
        values.append(data_classes[key])
        explode.append(0.05)

    # plt.figure(figsize=(12, 12))
    plt.axes(aspect=1)  # set this , Figure is round, otherwise it is an ellipse
    chart, l_text, p_text = plt.pie(
        x=values,
        labels=None,
        explode=explode,
        autopct='%3.1f %%',  # autopct，圆里面的文本格式，%3.1f%%表示小数有三位，整数有一位的浮点数
        shadow=True,
        labeldistance=1.1,  # labeldistance，文本的位置离远点有多远，1.1指1.1倍半径的位置
        startangle=30,  # staring angel
        pctdistance=0.8,  # pctdistance，百分比的text离圆心的距离
        radius=1
            )

    plt.title('ISV SUPPORT JAVA/.NET Post Distribution')
    plt.legend(chart, labels, loc='upper left', bbox_to_anchor=(-0.4, 1))
    plt.savefig(os.path.join(configure.OUTPUT_PATH, 'pie.png'))
    plt.show()


def bar_chart_pyplot():
    d = auto_zendesk_db.AutoZendeskDB("isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")
    post_data = d.get_isv_posts_data(['id', 'topic_id'])
    topic_data = d.get_isv_topics_data()

    topics = {}
    for topic in topic_data:
        topics[topic[0]] = topic[3]

    total = 0
    data_classes = {}
    for post in post_data:
        key = topics[post[1]]
        if key in data_classes.keys():
            data_classes[key] += 1
        else:
            data_classes[key] = 1
        total += 1

    labels = list()
    for key in data_classes.keys():
        labels.append(key)

    x_pos = numpy.arange(len(labels))
    x_pos = x_pos * 2
    y_pos = list()
    for o in labels:
        y_pos.append(data_classes[o])

    plt.figure(figsize=(6, 7))
    width = 1

    plt.bar(x_pos, y_pos, width, align='center', alpha=0.5)
    plt.xticks(x_pos, labels, rotation=-90, fontsize=8)
    # plt.xlabel('Topics')
    plt.ylabel('Number of Tickets')
    plt.title('Number of Tickets of Topics', bbox={'facecolor': '0.8', 'pad': 5})
    for x, y in zip(x_pos, y_pos):
        plt.text(x, y+0.5, '%.0f' % y, ha='center', va='bottom', fontsize=8)
    plt.savefig(os.path.join(configure.OUTPUT_PATH, "bar.png"))
    plt.show()


def time_bar_chart_pyplot():
    d = auto_zendesk_db.AutoZendeskDB("isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")
    post_data = d.get_isv_posts_data(['id', 'created_at_str'])
    time_classify = dict()
    for data in post_data:
        temp = data[1][:7]
        if temp in time_classify.keys():
            time_classify[temp] += 1
        else:
            time_classify[temp] = 1

    labels = list(time_classify.keys())
    labels.sort()

    x_pos = numpy.arange(len(labels))
    x_pos = x_pos * 2
    y_pos = list()
    for o in labels:
        y_pos.append(time_classify[o])

    plt.figure(figsize=(6, 5.5))
    width = 1

    plt.bar(
        x_pos,
        y_pos,
        width,
        align='center',
        alpha=0.5
    )
    plt.xticks(x_pos, labels, rotation=-90, fontsize=8)
    # plt.xlabel('Topics')
    plt.ylabel('Number of Tickets')
    plt.title('Number of Tickets by Month', bbox={'facecolor': '0.8', 'pad': 5})
    for x, y in zip(x_pos, y_pos):
        plt.text(x, y+0.5, '%.0f' % y, ha='center', va='bottom', fontsize=8)
    plt.savefig(os.path.join(configure.OUTPUT_PATH, "time_bar.png"))
    plt.show()


