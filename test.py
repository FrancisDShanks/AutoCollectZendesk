import auto_zendesk

if __name__ == "__main__":
    a = auto_zendesk.AutoZendesk(r'xufan.du@hp.com', r'Dxf352985861!',
                                 r"C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe",
                                 "isv_zendesk", "postgres", "Dxf3529!", "127.0.0.1", "5432")

    a.build_comments_postgresql()

