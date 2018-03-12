# AutoCollectPostsFromZendesk
Author: Francis Xufan Du      
Email:xufan.du@gmail.com

This is a python script to auto collect posts from zendesk api with Chrome, parse the json data and store in an excel file for statistic. 
Requirement:
  - Python 3 is needed.
  - xlwt and selenium module are needed.
      - pip install xlwt(selenium)
  - You will need an 'Username'-'Password' pair in order to retrieve data from Zendesk Api.
  - You will need install a chromedriver.exe to the folder hold chrome.exe.

  Usage:
  ```python
    a.drop_all_table_postgresql()
    a.collect_posts_and_comments()
    a.build_posts_excel()
    a.build_comments_excel()
```
