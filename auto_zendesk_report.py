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

import src.auto_zendesk_data_processing as data_processing
import src.auto_zendesk_db as db
import datetime


def run_visualization():
    data_processing.pie_chart_pyplot()
    data_processing.bar_chart_pyplot()
    data_processing.time_bar_chart_pyplot()


def report_filename(extents):
    if extents == 'md':
        filename = 'markdown_report_'
    else:
        filename = 'html_report_'
    date = str(datetime.date.today())
    extents = '.' + extents
    return filename + date + extents


def get_data_from_db():
    d = db.AutoZendeskDB("isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")
    return d.report_data()


def build_chart3(data):
    res3 = '<tr>'
    res3 += '<th>Post ID</th>'
    res3 += '<th>Post Title</th>'
    res3 += '<th>Topic</th>'
    res3 += '<th>Post Status</th>'
    res3 += '<th>Days From Last Response</th>'
    res3 += '<th>Last Responder</th>'
    res3 += '</tr>'
    res2 = res3
    cnt3 = 0
    cnt2 = 0
    for row in data:
        if row[4] <= 1:
            res2 += '<tr>'
            cnt2 += 1
            for col_n in range(len(row)):
                res2 += '<td>'
                if col_n == 0:
                    res2 += '#'
                res2 += str(row[col_n])

                res2 += '</td>'
            res2 += '</tr>'
        else:

            res3 += '<tr>'
            cnt3 += 1
            for col_n in range(len(row)):
                res3 += '<td>'
                if col_n == 0:
                    res3 += '#'
                res3 += str(row[col_n])

                res3 += '</td>'
            res3 += '</tr>'
    res3 = '<table border ="1">' + res3 + '</table>'
    res2 = '<table border ="1">' + res2 + '</table>'
    return res2, cnt2, res3, cnt3


def build_chart4(data):
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
    data = get_data_from_db()
    with open(report_filename('md'), 'w', newline=enter) as file:
        file.write("Hello Scott," + enter + enter)
        file.write("Here is the Report of the ISV SDK Support status." + enter)
        file.write("### 0. Priority Escalations:" + enter)
        file.write("*This issue is not in Forum as a post, but handled through email directly.*" + enter)

        file.write("### 1. Current New Posts:" + enter)
        file.write("*Current New (no initial response) Posts*" + enter + enter)
        file.write("*Note: Team is performing internal discussion on this issue and decide how to response to partner.*"
                   + enter)

        [chart2, cnt2, chart3, cnt3] = build_chart3(data)
        file.write("### 2. Post handled: " + str(cnt2) + enter)
        file.write("*Posts looked at / responded to in last 24 hours*" + enter)
        file.write(enter + chart2 + enter + enter)



        file.write("### 3. Posts No Response: " + str(cnt3) + enter)
        file.write("*Current Open Posts that have no response within 24 hours*" + enter)
        file.write(enter + chart3 + enter + enter)
        file.write("These Posts are currently Open or being worked on, but without responding within in 24 hours. "
                   "We will look into these and take action accordingly.  " + enter)
        file.write("*We are looking into and trying to clean up the post that without no response/update for more than "
                   "15 days. (some of them are very old and has no update for months)*" + enter)

        [chart4, cnt4, chart5, cnt5] = build_chart4(data)
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
        # file.write("![img](pie_chart.svg)" + enter + enter)

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

def get_header_css():
    res = """
    <!doctype html>
    <html>
    <head>
    <meta charset='UTF-8'><meta name='viewport' content='width=device-width initial-scale=1'>
    <title>markdown_report_2018-03-30.md</title>
    <link href='https://raw.githubusercontent.com/slashfoo/lmweb/master/style/latinmodern-mono-light.css' rel='stylesheet' type='text/css' />
    <link href="isv.css" rel="stylesheet" type="text/css" />
    </head>
    """
    return res

def build_html_report():
    enter = '<br/>'
    data = get_data_from_db()
    with open(report_filename('html'), 'w', newline='\n') as file:
        file.write(get_header_css())
        file.write("<body class='typora-export'><div id='write' class='is-node'>")
        file.write("<p>Hello Scott,</p>")
        file.write("<p>Here is the Report of the ISV SDK Support status.</p>")
        file.write("<h3><a name='header-n4' class='md-header-anchor '></a>0. Priority Escalations:</h3>")
        file.write("<p><em>This issue is not in Forum as a post, but handled through email directly.</em></p>")

        file.write("<h3><a name='header-n7' class='md-header-anchor '></a>1. Current New Posts:</h3>")
        file.write("<p><em>Current New (no initial response) Posts</em></p>")
        file.write("<p><em>Note: Team is performing internal discussion on this issue and decide how to response "
                   "to partner.</em></p>")

        [chart2, cnt2, chart3, cnt3] = build_chart3(data)
        file.write("<h3><a name='header-n12' class='md-header-anchor '></a>2. Post handled: " + str(cnt2) + "</h3>")
        file.write("<p><em>Posts looked at / responded to in last 24 hours</em></p>")
        file.write("<p>" + chart2 + "</p>")

        file.write("<h3><a name='header-n18' class='md-header-anchor '></a>3. Posts No Response: "
                   + str(cnt3) + "</h3>")
        file.write("<p><em>Current Open Posts that have no response within 24 hours</em></p>")
        file.write("<p>" + chart3 + "</p>")
        file.write("<p>These Posts are currently Open or being worked on, but without responding within in 24 hours. "
                   "We will look into these and take action accordingly.<br/>")
        file.write("<em>We are looking into and trying to clean up the post that without no response/update \
        for more than 15 days. (some of them are very old and has no update for months)</em></p>")

        [chart4, cnt4, chart5, cnt5] = build_chart4(data)
        file.write("<h3><a name='header-n24' class='md-header-anchor '></a>4. Internal & External Pending Posts: "
                   + str(cnt4) + "</h3>")
        file.write("<p><em>Posts waiting on internal external response (waiting for FW, RDL, SDK, etc...)</em></p>")
        file.write("<p>" + chart4 + "</p>")

        file.write("<h3><a name='header-n31' class='md-header-anchor '></a>5. Partner Pending Posts: "
                   + str(cnt5) + "</h3>")
        file.write("<p><em>Posts waiting on Partner Response</em></p>")
        file.write(enter + chart5 + enter + enter)
        file.write("<p>These posts are waiting for Partner's feedback on query about more info. "
                   "Team will monitor and continue work once partner provided feedback.</p>")
        file.write("<h3><a name='header-n37' class='md-header-anchor '></a>6. Overview of Posts from Forum</h3>")
        file.write("<p><em>OXPd Pro and Samsung Printing SDK are not included. "
                   "Status of Some of Posts are TBD which are still being reviewed.</em></p>")
        file.write("<p><img src='pie.png' alt='img'/></p>")
        file.write("<p><img src='bar.png' alt='img'/></p>")
        file.write("<p><img src='time_bar.png' alt='img'/></p>")
        # file.write("![img](pie_chart.svg)" + enter + enter)

        file.write("<p>Notes:</p>")
        file.write("<ol start=''>")
        file.write('<li>"Closed" are the posts with Closed status or marked as Answered.</li>')
        file.write('<li>"Solved" are the posts that we responded partner with a solution/workaround.</li>')
        file.write('<li>"Open" are the posts that need further response from support team.</li>')
        file.write('<li>"New" are the ones just created and no response made yet.</li>')
        file.write('<li>"Ongoing Work"" are the ones that team are working on for a solution.</li>')
        file.write('<li>"External pending" refers to post is waiting on feedback from other teams in HP '
                   '(FW, RDL, SDK, etc.)</li>')
        file.write('<li>"Internal Pending" refers to post is waiting on result of Support team.( Response '
                   'is pending for approval, Internal discussion, Article/Documentation/Portal updates, etc.)</li>')
        file.write('<li>"Partner Pending"" refers to posts that waiting on feedback from partner to continue.</li>')
        file.write('<li>"Not a Support Ticket"  refers to posts that submitted by HP guys and mostly for '
                   'discussion on some topics. Not from partners.</li></ol>')
        file.write("<p>Please let us know if any comments on this report. Thanks.</p>")
        file.write("<p>Best Regards</p>")
        file.write("<p>Denon</p>")
        file.write("<p>Beyondsoft Corporation</p>")
        file.write("</div>")
        file.write("</body></html>")


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

def run_report():
    run_visualization()
    build_markdown_report()
    build_html_report()

if __name__ == "__main__":
    run_report()











