# -*- coding: utf-8 -*-
"""
   Copyright 2018 Francis Xufan Du

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

	@author: Francis Xufan Du
	@email: duxufan@beyondsoft.com xufan.du@gmail.com
	@Version: 0.1-Beta
"""

#core mods
import codecs
import os
import json
import xlwt
import time
import re

#3rd party mods
from selenium import webdriver




def loadJson(fileName):
    with open(fileName, encoding='utf8') as json_file:
        data = json.load(json_file)
    return data



if __name__ == "__main__":
    save_path = os.path.abspath('.') + '\\'
    browser = webdriver.Chrome(r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')  # Optional argument, if not specified will search path.
    browser.get(r'https://developers.hp.com/user/login?destination=hp-zendesk-sso')
    search_box = browser.find_element_by_name('name')
    search_box.send_keys(r'')
    search_box = browser.find_element_by_name('pass')
    search_box.send_keys(r'')
    search_box.submit()
    browser.get(r'https://jetadvantage.zendesk.com/hc/en-us')
    time.sleep(15)
    for page_count in range(1,10):
        js = 'window.open("https://jetadvantage.zendesk.com//api/v2/community/posts.json?page=' + str(page_count) + '");'
        browser.execute_script(js)
        base_handler = browser.current_window_handle
        all_handler = browser.window_handles
        for handler in all_handler:
            if handler != base_handler:
                browser.switch_to_window(handler)
                
                
                file_name = 'post' + str(page_count) + '.json'
                full_path = save_path + file_name
                file_object = codecs.open(full_path, 'w', 'utf-8')
                raw_data = browser.page_source
                
                dr = re.compile(r'<[^>]+>',re.S)
                dd = dr.sub('',raw_data)
                
                
                file_object.write(dd)
                file_object.close()
                browser.close()
        browser.switch_to_window(base_handler)
    browser.quit()
    
    saveFileName = save_path + 'posts.xls'
    if os.path.exists(saveFileName):
        os.remove(saveFileName)
    wb = xlwt.Workbook()
    ws = wb.add_sheet('All Posts')
    rowCount = 0    
    for i in range(1,10):       

        
    
        fileName = save_path + 'post' + str(i) + '.json'
        data = {}
        data = loadJson(fileName)
        
        posts = data['posts']
        
        if not rowCount:
            count = 0
            for key in posts[0].keys():
                ws.write(0, count, key)
                count += 1
            
    
        for post in posts:
            rowCount +=1 
            count = 0
            for key in post.keys():
                if key == 'details' and len(post[key])>32768:
                    post[key] = post[key][:32767]
                
                if key == 'id' or key == 'author_id' or key == 'topic_id':
                    post[key] = '#' + str(post[key])
                                       
                ws.write(rowCount, count, str(post[key]))
                count += 1 

    wb.save(saveFileName)
    
    for page_count in range(1,10):
        file_name = 'post' + str(page_count) + '.json'
        full_path = save_path + file_name
        if os.path.exists(full_path):
            os.remove(full_path)