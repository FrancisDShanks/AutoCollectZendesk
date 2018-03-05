import auto_zendesk_crawling
import auto_zendesk_db
import auto_zendesk_helper


def run_crawling():
    c = auto_zendesk_crawling.AutoZendeskCrawling(r'xufan.du@hp.com', r'Dxf352985861!',
                                                  r"C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe")
    c.run_all()


def run_database():
    d = auto_zendesk_db.AutoZendeskDB("isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")
    d.run_all()


def run_helper():
    e = auto_zendesk_helper.AutoZendeskHelper()
    e.remove_all_json_files()
    e.move_excel()


if __name__ == "__main__":
    run_database()