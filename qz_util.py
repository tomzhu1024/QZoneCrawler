import json
import os
import pickle
import random
import re
import time

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def log(message: str) -> None:
    print('[%s]\t%s' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        message
    ))


def generate_g_tk(cookie: dict) -> int:
    hashes = 5381
    for char in cookie['p_skey']:
        hashes += (hashes << 5) + ord(char)
    return hashes & 0x7fffffff


def login_qzone(username: str, password: str) -> (dict, int, str):
    action_delay = 0.5  # second
    exp_wait = 30  # seconds
    wd = webdriver.Chrome("chromedriver.exe")
    wd.get("http://i.qq.com/")
    wd.switch_to.frame(
        WebDriverWait(wd, exp_wait).until(EC.presence_of_element_located((By.XPATH, "//iframe[@id='login_frame']"))))
    wd.find_element_by_id('switcher_plogin').click()
    time.sleep(action_delay)
    # wd.find_element_by_id('u').clear()
    # wd.find_element_by_id('p').clear()
    # time.sleep(action_delay)
    wd.find_element_by_id('u').send_keys(username)
    time.sleep(action_delay)
    wd.find_element_by_id('p').send_keys(password)
    time.sleep(action_delay)
    wd.find_element_by_id('login_button').click()
    # restore to parent frame
    wd.switch_to.parent_frame()
    # wait until page loads
    WebDriverWait(wd, exp_wait).until(EC.presence_of_element_located((By.XPATH, "//div[@id='$1_substitutor_content']")))
    # get cookie
    cookie = dict()
    for element in wd.get_cookies():
        cookie[element['name']] = element['value']
    # get g_tk
    g_tk = generate_g_tk(cookie)
    # get g_qzonetoken
    src = wd.page_source
    g_qzonetoken = re.search(r'window\.g_qzonetoken = \(function\(\)\{ try\{return "(.*?)";\} catch\(e\)', src).group(1)
    wd.quit()
    return cookie, g_tk, g_qzonetoken


header = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
    "cache-control": "max-age=0",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
}


def generate_random() -> str:
    """
    生成随机数
    :return: 一个在0-1之间的随机数的字符串
    """
    return str(random.random())


def generate_unikey(dst_uin: str, tid: str) -> str:
    """
    生成unikey参数（说说地址）
    :param dst_uin: 目标QQ号
    :param tid: 说说ID
    :return: unikey
    """
    return 'http://user.qzone.qq.com/%s/mood/%s' % (dst_uin, tid)


def parse_response(data: str) -> dict:
    """
    探测string中json数据的边界并解析
    :param data: 原数据
    :return: json dict
    """
    start_pos = 0
    end_pos = len(data) - 1
    while start_pos < end_pos and data[start_pos] != '{':
        start_pos += 1
    while start_pos < end_pos and data[end_pos] != '}':
        end_pos -= 1
    return json.loads(data[start_pos:end_pos + 1])


class QZDataCatcher:
    def __init__(self):
        self.username = ''
        self.password = ''
        self.cookie = {}
        self.g_tk = 0
        self.g_qzonetoken = ''
        self.http_sen = None
        self.min_request_interval = 0.5  # seconds
        self.last_request = 0

    def acquire_sleep_timer(self) -> None:
        """
        占用计时器，在需要控制频率的操作之前调用
        :return: none
        """
        current_time = time.time()
        if current_time - self.last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - (current_time - self.last_request))

    def release_sleep_timer(self) -> None:
        """
        释放计时器，在需要控制频率的操作结束后调用
        :return: none
        """
        self.last_request = time.time()

    def auth(self):
        """
        模拟用户登录并获取cookie
        :return: none
        """
        # load config
        with open('qz_auth.cfg', 'r') as fs:
            self.username = fs.readline().strip()
            self.password = fs.readline().strip()
        # auth from cache
        cache_filename = 'qz_cookie.dat'
        if os.path.exists(cache_filename):
            with open(cache_filename, 'rb') as fs:
                cache_time = pickle.load(fs)
                self.cookie = pickle.load(fs)
                self.g_tk = pickle.load(fs)
                self.g_qzonetoken = pickle.load(fs)
        # check timeliness of cache
        if not os.path.exists(cache_filename) or time.time() - cache_time >= 3600:
            # auth from web
            self.cookie, self.g_tk, self.g_qzonetoken = login_qzone(self.username, self.password)
            # write to cache
            with open(cache_filename, 'wb') as fs:
                pickle.dump(time.time(), fs)
                pickle.dump(self.cookie, fs)
                pickle.dump(self.g_tk, fs)
                pickle.dump(self.g_qzonetoken, fs)
            log('auth完成：selenium模式')
        else:
            log('auth完成：pickle模式')
        # initiate requests session
        self.http_sen = requests.session()
        # self.http_sen.verify = False

    def request_emotion_list(self, dst_uin: str, pos: int, num: int = 20) -> dict:
        """
        请求说说列表（Web版协议）
        :param dst_uin: 目标QQ号
        :param pos: 说说开始位置
        :param num: 说说数量（默认20）
        :return: json dict
        """
        param = {
            'uin': dst_uin,
            'ftype': 0,
            'sort': 0,
            'pos': pos,
            'num': num,
            'replynum': 100,
            'g_tk': self.g_tk,
            'callback': '_preloadCallback',
            'code_version': 1,
            'format': 'jsonp',
            'need_private_comment': 1,
            'qzonetoken': self.g_qzonetoken
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get(
            "https://user.qzone.qq.com/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6", headers=header,
            cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_emotion_detail(self, dst_uin: str, tid: str, pos: int, num: int = 20) -> dict:
        """
        请求特定说说的详细信息（Web版协议），当一条说说评论数量不为0，并且pos值大于等于该说说的评论数量时，请求将返回HTML数据而非JSON
        :param dst_uin: 目标QQ号
        :param tid: 说说ID
        :param pos: 评论开始位置
        :param num: 评论数量（默认20）
        :return: json dict
        """
        param = {
            'uin': dst_uin,
            'tid': tid,
            't1_source': 1,
            'ftype': 0,
            'sort': 0,
            'pos': pos,
            'num': num,
            'g_tk': self.g_tk,
            'callback': '_preloadCallback',
            'code_version': 1,
            'format': 'jsonp',
            'need_private_comment': 1,
            'qzonetoken': self.g_qzonetoken,
            # 为防止content被截断，增加了POST协议中的参数，测试可行，但不排除有潜在的问题
            'no_trunc_con': 1
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get(
            "https://user.qzone.qq.com/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msgdetail_v6", headers=header,
            cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_like_list(self, dst_uin: str, tid: str, begin_uin: str = '0', query_count: int = 60) -> dict:
        """
        请求特定说说的赞列表（Web版协议），一般来说，转发说说应该请求源说说，不过反之亦可
        :param dst_uin: 目标QQ号
        :param tid: 说说ID
        :param begin_uin: 上一次查询结果中最后一个uin（第一次查询填'0'）
        :param query_count: 查询结果数量（默认60）
        :return: json dict
        """
        param = {
            'uin': self.username,
            'unikey': generate_unikey(dst_uin, tid),
            'begin_uin': begin_uin,
            'query_count': query_count,
            'if_first_page': 1 if begin_uin == '0' else 0,
            'g_tk': self.g_tk,
            'qzonetoken': self.g_qzonetoken
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get(
            "https://user.qzone.qq.com/proxy/domain/users.qzone.qq.com/cgi-bin/likes/get_like_list_app", headers=header,
            cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_like_list_mobile(self, dst_uin: str, tid: str, attach_info: str = '') -> dict:
        """
        请求特定说说的赞列表（手机QQ协议），一般来说，转发说说应该请求转发说说，不过反之亦可
        :param dst_uin: 目标QQ号
        :param tid: 说说ID
        :param attach_info: 上次返回结果中的attach_info项（第一次请求填''）
        :return: json dict
        """
        param = {
            't': generate_random(),
            'g_tk': self.g_tk
        }
        data = {
            'uin': self.username,
            'unikey': generate_unikey(dst_uin, tid),
            'attach_info': attach_info,
            'hostuin': dst_uin,
            'format': 'json',
            'inCharset': 'utf-8',
            'outCharset': 'utf-8'
        }
        self.acquire_sleep_timer()
        response = self.http_sen.post("https://h5.qzone.qq.com/webapp/json/mobile_extra_protocol/getLikeList",
                                      headers=header, cookies=self.cookie, params=param, data=data)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_friend_and_group_list(self) -> dict:
        """
        请求好友和群聊列表（Web版协议）
        :return: json dict
        """
        param = {
            'uin': self.username,
            'follow_flag': 0,
            'groupface_flag': 0,
            'fupdate': 1,
            'g_tk': self.g_tk,
            'g_qzonetoken': self.g_qzonetoken
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get(
            "https://user.qzone.qq.com/proxy/domain/r.qzone.qq.com/cgi-bin/tfriend/friend_show_qqfriends.cgi",
            headers=header, cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_pics_list(self, dst_uin: str, tid: str) -> dict:
        """
        请求所有图片列表（Web版协议）
        :param dst_uin: 目标QQ号
        :param tid: 说说ID
        :return: json dict
        """
        r = generate_random()
        param = {
            'r': r,
            'tid': tid,
            'uin': dst_uin,
            't1_source': '1',
            'random': r,
            'g_tk': self.g_tk,
            'qzonetoken': self.g_qzonetoken
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get(
            "https://h5.qzone.qq.com/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_get_pics_v6", headers=header,
            cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_user_info(self, dst_uin: str) -> dict:
        """
        请求用户个人信息
        :param dst_uin: 目标QQ号
        :return: json dict
        """
        param = {
            'uin': dst_uin,
            'vuin': self.username,
            'fupdate': 1,
            'rd': generate_random(),
            'g_tk': self.g_tk,
            'qzonetoken': self.g_qzonetoken
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get(
            "https://h5.qzone.qq.com/proxy/domain/base.qzone.qq.com/cgi-bin/user/cgi_userinfo_get_all", headers=header,
            cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def request_message_board(self, dst_uin: str, start: int, num: int = 10) -> dict:
        """
        请求留言板
        :param dst_uin: 目标QQ号
        :param start: 留言开始位置
        :param num: 留言数量（默认10）
        :return: json dict
        """
        param = {
            'uin': self.username,
            'hostUin': dst_uin,
            'start': start,
            's': generate_random(),
            'format': 'jsonp',
            'num': num,
            'inCharset': 'utf-8',
            'outCharset': 'utf-8',
            'g_tk': self.g_tk,
            'qzonetoken': self.g_qzonetoken
        }
        self.acquire_sleep_timer()
        response = self.http_sen.get("https://user.qzone.qq.com/proxy/domain/m.qzone.qq.com/cgi-bin/new/get_msgb",
                                     headers=header, cookies=self.cookie, params=param)
        self.release_sleep_timer()
        return parse_response(response.content.decode('utf-8'))

    def save_as_file(self, url: str, filename: str, send_cookies: bool = False) -> bool:
        """
        保存指定URL的数据为文件
        :param url: 请求的网络地址
        :param filename: 保存的文件名
        :param send_cookies: 是否发送cookie
        :return: 是否下载成功
        """
        try:
            if send_cookies:
                response = self.http_sen.get(url, headers=header, cookies=self.cookie)
            else:
                response = self.http_sen.get(url, headers=header)
            with open(filename, 'wb') as fs:
                fs.write(response.content)
            return True
        except Exception as ex:
            log('保存到文件操作失败')
            print('错误URL：%s\n错误信息：%s' % (url, str(ex)))
        return False


class StatusCodeError(Exception):
    def __init__(self, message: str, code: int = 0, subcode: int = 0):
        super(StatusCodeError, self).__init__(message)
        self.code = code
        self.subcode = subcode
        self.message = message


class UrlEmptyError(Exception):
    pass
