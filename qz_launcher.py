from qz_crawler import *

if __name__ == '__main__':
    dc = QZDataCatcher()
    dc.auth()
    crawl_emotion_by_uin(dc, '1234567890')
