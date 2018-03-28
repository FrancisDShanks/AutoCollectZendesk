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
@Version: 	03/2018 0.7-Beta    add auto_zendesk_report.py module to generate reports based on MarkDown
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
import auto_zendesk_data_processing
import auto_zendesk_db
import datetime


def run_vitualization():
    auto_zendesk_data_processing.pie_chart_pyplot()
    auto_zendesk_data_processing.bar_chart_pyplot()
    auto_zendesk_data_processing.time_bar_chart_pyplot()


def mardown_report_filename():
    filename = 'markdown_report_'
    date = str(datetime.date.today())
    extents = '.md'
    return filename + date + extents


def build_chart():
    d = auto_zendesk_db.AutoZendeskDB("isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")

    data = d.markdown()
    res = '<tr>'
    res += '<th>Post ID</th>'
    res += '<th>Post Title</th>'
    res += '<th>Topic</th>'
    res += '<th>Post Status</th>'
    res += '<th>Days From Last Response</th>'
    res += '<th>Last Responder</th>'
    res += '</tr>'
    for row in data:
        res += '<tr>'
        for col_n in range(len(row)):
            res += '<td>'
            if col_n == 0:
                res += '#'
            res += str(row[col_n])

            res += '</td>'
        res += '</tr>'
    res = '<table border ="1">' + res + '</table>'
    return res, len(data), data


def reports_tables(data):
    res4 = '<tr>'
    res4 += '<th>Post ID</th>'
    res4 += '<th>Post Title</th>'
    res4 += '<th>Topic</th>'
    res4 += '<th>Post Status</th>'
    res4 += '<th>Days From Last Response</th>'
    res4 += '<th>Last Responder</th>'
    res4 += '</tr>'
    res5 = res4
    cnt4 = 0
    cnt5 = 0

    for row in data:
        if row[3] in ['InternalPending', 'ExternalPending']:
            cnt4 += 1
            res4 += '<tr>'
            for col_n in range(len(row)):
                res4 += '<td>'
                if col_n == 0:
                    res4 += '#'
                res4 += str(row[col_n])
                res4 += '</td>'
        if row[3] == 'PartnerPending':
            cnt5 += 1
            res5 += '<tr>'
            for col_n in range(len(row)):
                res5 += '<td>'
                if col_n == 0:
                    res5 += '#'
                res5 += str(row[col_n])
                res5 += '</td>'

        res4 += '</tr>'
        res5 += '</tr>'
    res4 = '<table border ="1">' + res4 + '</table>'
    res5 = '<table border ="1">' + res5 + '</table>'
    return res4, cnt4, res5, cnt5


def build_markdown_report():
    enter = '\n'
    with open(mardown_report_filename(), 'w', newline=enter) as file:
        file.write("Hello Scott," + enter + enter)
        file.write("Here is the Report of the ISV SDK Support status." + enter)
        file.write("### 0. Priority Escalations:" + enter)
        file.write("*This issue is not in Forum as a post, but handled through email directly.*" + enter)

        file.write("### 1. Current New Posts:" + enter)
        file.write("*Current New (no initial response) Posts*" + enter + enter)
        file.write("*Note: Team is performing internal discussion on this issue and decide how to response to partner.*"
                   + enter)
        file.write("### 2. Post handled: 5" + enter)
        file.write("*Posts looked at / responded to in last 24 hours*" + enter)

        [chart, cnt, data] = build_chart()
        file.write("### 3. Posts No Response: " + str(cnt) + enter)
        file.write("*Current Open Posts that have no response within 24 hours*" + enter)
        file.write(enter + chart + enter + enter)
        file.write("These Posts are currently Open or being worked on, but without responding within in 24 hours. "
                   "We will look into these and take action accordingly.  " + enter)
        file.write("*We are looking into and trying to clean up the post that without no response/update for more than "
                   "15 days. (some of them are very old and has no update for months)*" + enter)

        [chart4, cnt4, chart5, cnt5] = reports_tables(data)
        file.write("### 4. Internal & External Pending Posts: " + str(cnt4) + enter)
        file.write("*Posts waiting on internal external response (waiting for FW, RDL, SDK, etc...)*" + enter)
        file.write(enter + chart4 + enter + enter)

        file.write("### 5. Partner Pending Posts: " + str(cnt5) + enter)
        file.write("*Posts waiting on Partner Response*" + enter + enter)
        file.write(enter + chart5 + enter + enter)
        file.write("These posts are waiting for Partner's feedback on query about more info. "
                   "Team will monitor and continue work once partner provided feedback." + enter)
        file.write("### 6. Overview of Posts from Forum" + enter)
        file.write("*OXPd Pro and Samsung Printing SDK are not included. "
                   "Status of Some of Posts are TBD which are still being reviewed.*" + enter + enter)
        file.write("![img](pie.png)" + enter + enter)
        file.write("![img](bar.png)" + enter + enter)
        file.write("![img](time_bar.png)" + enter + enter)
        file.write("![img](pie_chart.svg)" + enter + enter)

        file.write("Notes:" + enter)
        file.write('   1.    "Closed" are the posts with Closed status or marked as Answered.' + enter)
        file.write('   2.    "Solved" are the posts that we responded partner with a solution/workaround.' + enter)
        file.write('   3.    "Open" are the posts that need further response from support team.' + enter)
        file.write('   4.    "New" are the ones just created and no response made yet.' + enter)
        file.write('   5.    "Ongoing Work"" are the ones that team are working on for a solution.' + enter)
        file.write('   6.    "External pending" refers to post is waiting on feedback from other teams in HP '
                   '(FW, RDL, SDK, etc.)' + enter)
        file.write('   7.    "Internal Pending" refers to post is waiting on result of Support team.( Response '
                   'is pending for approval, Internal discussion, Article/Documentation/Portal updates, etc.)' + enter)
        file.write('   8.    "Partner Pending"" refers to posts that waiting on feedback from partner to continue.'
                   + enter)
        file.write('   9.    "Not a Support Ticket"  refers to posts that submitted by HP guys and mostly for '
                   'discussion on some topics. Not from partners. ' + enter + enter)
        file.write("Please let us know if any comments on this report. Thanks." + enter + enter)
        file.write("Best Regards" + enter + enter)
        file.write("Denon" + enter + enter)
        file.write("Beyondsoft Corporation" + enter)


def markdown_to_html():
    import markdown
    from markdown.extensions.wikilinks import WikiLinkExtension

    input_file = open("markdown_report_2018-03-28.md", mode="r", encoding="utf-8")
    text = input_file.read()
    html = markdown.markdown(text,
                             output_format='html5',
                             extensions=['markdown.extensions.toc',
                                         WikiLinkExtension(base_url='https://en.wikipedia.org/wiki/',
                                                           end_url='#Hyperlinks_in_wikis'),
                                         'markdown.extensions.sane_lists',
                                         'markdown.extensions.codehilite',
                                         'markdown.extensions.abbr',
                                         'markdown.extensions.attr_list',
                                         'markdown.extensions.def_list',
                                         'markdown.extensions.fenced_code',
                                         'markdown.extensions.footnotes',
                                         'markdown.extensions.smart_strong',
                                         'markdown.extensions.meta',
                                         'markdown.extensions.nl2br',
                                         'markdown.extensions.tables'])

    output_file = open("foo.html", "w",
                       encoding="utf-8",
                       errors="xmlcharrefreplace"
                       )
    output_file.write(html)


if __name__ == "__main__":
    run_vitualization()
    build_markdown_report()
    markdown_to_html()











