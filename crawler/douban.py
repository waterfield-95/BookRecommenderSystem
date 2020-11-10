"""使用前记得更新豆瓣cookies"""

from lxml import etree
import requests
import re
import random
from fake_useragent import UserAgent
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import sys
from requests.exceptions import HTTPError, ConnectionError
import json
import ipdb
from pymongo import MongoClient
from redis import StrictRedis


def get_data_from_mongo(filter: dict, key: str) -> list:
    isbn_list = []
    with MongoClient("mongodb://localhost:27017") as client:
        train = client["seaeverit"]["book_train"]
        for item in train.find(filter):
            isbn_list.append(item[key])
    return isbn_list


def store_in_mongo(books_info):
    """
    books_info: {ISBN: book_info_dict}
    """
    with MongoClient("mongodb://localhost:27017") as client:
        train = client["seaeverit"]["book_train"]
        for isbn, info_dict in books_info.items():
            train.update_one({"ISBN": isbn}, {"$set": info_dict})


class DoubanSpider(object):
    """输入isbn列表，可获取对应书籍的信息字典，信息包括标题，评分，内容简介，目录，标签，url"""
    def __init__(self, use_proxy=False):
        self.all_books_info = {}
        self.ua = UserAgent()
        self.use_proxy = use_proxy
        self.rconn = StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)

    def proxies(self):

        # 代理服务器
        proxyHost = "dyn.horocn.com"
        proxyPort = "50000"

        # 代理隧道验证信息
        proxyUser = "3VQR1682578059975108"
        proxyPass = "xE48fs9tpQz6LPmz"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        # 设置 http和https访问都是用HTTP代理
        proxies = {
            "http": proxyMeta,
            "https": proxyMeta,
        }
        return proxies

    def get_proxies(self):
        """
        获取代理ip
        :return: 随机ip
        """

        proxy_ip = self.rconn.srandmember('proxy_pool')
        proxies = {
            'http': 'http://' + proxy_ip,
            'https': 'https://' + proxy_ip,
        }
        return proxies

    def delete_invalid_ip(self, proxies):
        """删除无效ip"""
        invalid_ip = re.search(r'http://(.*)$', proxies['http']).group(1)
        self.rconn.srem('proxy_pool', invalid_ip)

    @staticmethod
    def get_book_details_url(isbn):
        """webdriver获取书籍详情页url"""

        # 设置外网ip
        # proxy = "127.0.0.1:1081"
        # options.add_argument("--proxy-server=http://{}".format(proxy))
        # browser = webdriver.Chrome(options=options)

        # options = webdriver.ChromeOptions()
        options = webdriver.FirefoxOptions()

        options.add_argument('--incognito')  # 无痕模式
        options.add_argument('--headless')  # 无界面模式

        # with webdriver.PhantomJS() as driver:     # 无界面，但selenium已经deprecated，推荐使用Firfox --headless
        with webdriver.Firefox(options=options) as driver:
        # with webdriver.Chrome(options=options) as driver:
            browser = driver.get("https://book.douban.com/")
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.ID, 'inp-query')))
            except TimeoutException as e:
                print("Timeout!")
                sys.exit()

            # 输入框填入isbn搜索目标图书
            input_box = driver.find_element_by_id("inp-query")
            input_box.clear()
            input_box.send_keys(isbn)
            driver.implicitly_wait(1)
            button = driver.find_element_by_xpath("//input[@type='submit']")
            button.click()
            driver.implicitly_wait(1)

            # 通过img标签获取链接地址，搜索返回第一项
            try:
                title = driver.find_element_by_class_name("title-text")
                return title.get_attribute("href")
            except NoSuchElementException:
                print("该书找不到！")
                return

    def get_response_text(self, url):
        """获取某网站的html页面
        """
        if url is None:
            return
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        }
        cookies = {
            'll': '118282',
            'bid': 'L010R6yyi8w',
            '__utmc': '30149280',
            'gr_user_id': '03cce2dc-e927-40c3-b2c7-3fc248abfcaa',
            '__yadk_uid': '1aNqBAtqGXDn2KEdNtqlHFnbKTG8hNuX',
            '_vwo_uuid_v2': 'D793FAC9C56B44E1AE5F33F751BA726EC|a196d126463d7740bd898895127d6fef',
            'douban-fav-remind': '1',
            '__utmz': '30149280.1604901038.19.11.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)',
            'ap_v': '0,6.0',
            'viewed': '35200801_35223212_30908520_1449352_30144978_35190899_35115625_30206117_27139846_30278792',
            '__gads': 'ID=a0f79d81392a0297-22f90c08a0c40002:T=1604901093:RT=1604901093:S=ALNI_Mbez051hBkdWqfUFc6njGuZoeRCgA',
            'gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03': 'b99d4d24-8dc0-4aa5-bdc6-6bed975ade19',
            'gr_cs1_b99d4d24-8dc0-4aa5-bdc6-6bed975ade19': 'user_id%3A0',
            'gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03_b99d4d24-8dc0-4aa5-bdc6-6bed975ade19': 'true',
            '_pk_ref.100001.3ac3': '%5B%22%22%2C%22%22%2C1604907119%2C%22https%3A%2F%2Fwww.douban.com%2F%22%5D',
            '_pk_id.100001.3ac3': '2406857cae7303bc.1604371247.19.1604907119.1604901093.',
            '_pk_ses.100001.3ac3': '*',
            '__utma': '30149280.171069040.1604371207.1604901038.1604907119.20',
            '__utmt_douban': '1',
            '__utmb': '30149280.1.10.1604907119',
            '__utmt': '1',
        }

        cookies['__utmb'] = '30149280.{}.10.1603168583'.format(random.choice([i for i in range(1, 10)]))

        retries = 3
        while retries > 0:
            try:
                if self.use_proxy:
                    time.sleep(1)
                    response = requests.get(url, headers=headers, cookies=cookies, proxies=self.proxies())
                else:
                    # 页面请求间隔5s
                    time.sleep(5)
                    response = requests.get(url, headers=headers, cookies=cookies)
                response.raise_for_status()
            except (HTTPError, ConnectionError):
                retries -= 1
                time.sleep(3)
                print("Abnormal request from your IP!")
            except Exception as e:
                retries -= 1
                print("Error when getting html: ", type(e), e)
                time.sleep(3)
            else:
                if ("检测到有异常请求从你的 IP 发出" or "呃...你想访问的页面不存在") in response.text:
                    print("IP异常，请登录使用豆瓣！")
                else:
                    return response.text
        sys.exit()

    @staticmethod
    def parse_details_page(html):
        """通过解析书籍的详情页，获取需要的书籍信息
        """
        if html is None:
            return
        book_info = {}
        try:
            info_dict = html.xpath("//script[@type='application/ld+json']/text()")
            book_info["豆瓣标题"] = json.loads(info_dict[0])["name"]
            book_url = json.loads(info_dict[0])["url"]
            book_info["豆瓣URL"] = book_url
            book_id = re.search(r'\d+', book_url).group()
            catalog_list = html.xpath("//div[@id='dir_{}_full']/text()".format(book_id))

            book_info["豆瓣目录"] = [] if len(catalog_list) == 0 \
                else [content.strip() for content in catalog_list if content.strip()]

            rating = html.xpath("//strong[@class='ll rating_num ']/text()")
            book_info['豆瓣评分'] = '' if len(rating) == 0 else rating[0].strip()

            book_info['内容简介'] = html.xpath("//div[@id='link-report']//div[@class='intro']/p/text()")
            book_info["豆瓣标签"] = html.xpath("//div[@id='db-tags-section']//a[@class='  tag']/text()")
        except Exception as e:
            print("Parse Error!\n", type(e), e)
            ipdb.set_trace()
        return book_info

    def parse_one_comments_page(self, html) -> list:
        """解析评论页获取评论编号从而获取全部评论"""
        if html is None:
            return []
        try:
            cid_list = html.xpath('//div[@class="review-list  "]/div/@data-cid')
        except Exception as e:
            print("Error when parsing comments page: ", type(e), e)
            ipdb.set_trace()
        else:
            return self.parse_xhr_comments(cid_list)

    def parse_xhr_comments(self, cid_list) -> list:
        """通过评论id_list中的所有评论"""
        comments = []
        if len(cid_list) == 0:
            return comments
        base_url = 'https://book.douban.com/j/review/{}/full'
        for i, cid in enumerate(cid_list):
            url = base_url.format(cid)
            content = self.get_response_text(url)
            comment = re.sub(r'<.*?>', '', json.loads(content)['html'])    # 去除html标签<...>
            comment = re.sub(r"\n|\t|&nbsp;|&amp;|&quot;", "", comment)   # 去除空白符
            comments.append(comment)
            print(i+1, ":\n", comment)
        return comments

    def get_comments(self, book_id: str, page_number: int = 1) -> list:
        """获取一本书的给定评论页数的评论"""
        all_comments = []
        base_url = 'https://book.douban.com/subject/{}/reviews?start='.format(book_id)
        for page in range(page_number):
            print("\n----------------------------Page {}----------------------------\n".format(page+1))
            comment_url = base_url + str(20*page)
            html = etree.HTML(self.get_response_text(comment_url))
            comments = self.parse_one_comments_page(html)
            if len(comments) == 0:
                break
            else:
                all_comments.extend(comments)
        return all_comments

    def get_book_info(self, isbn):
        """获取书籍信息：标题，豆瓣评分，目录，内容简介，标签"""
        book_info = {}
        url = self.get_book_details_url(isbn)
        if url == None:
            return {}
        html = etree.HTML(self.get_response_text(url))
        book_info.update(self.parse_details_page(html))
        # book_id = re.search(r'\d+', url).group()
        # book_info.update(self.get_comments(book_id))
        print(book_info)
        return book_info

    def get_all_books_info(self, isbn_list) -> dict:
        """获取列表中所有图书的网络信息

        返回字典{'isbn': {info}, ...}
        """
        for i, isbn in enumerate(set(isbn_list)):
            print(i+1, ": ", isbn)
            self.all_books_info[isbn] = self.get_book_info(isbn)
        return self.all_books_info

    def add_comments_info_to_mongo(self, url_list):
        comments_dict = {}
        with MongoClient("mongodb://localhost:27017") as client:
            train = client["seaeverit"]["book_train"]
            for url in url_list:
                book_id = re.search(r'\d+', url).group()
                print('\n', url, '\n')
                comments_dict[url] = self.get_comments(book_id)
                if len(comments_dict[url]) == 0:
                    continue
                else:
                    train.update_one({"豆瓣URL": url}, {"$set": {'豆瓣评论': comments_dict[url]}})


if __name__ == "__main__":
    # urls = get_data_from_mongo(filter={}, key='豆瓣URL')
    urls = get_data_from_mongo(filter={'豆瓣评论': {"$exists": False}}, key='豆瓣URL')
    s = DoubanSpider()
    s.add_comments_info_to_mongo(urls[5:])
