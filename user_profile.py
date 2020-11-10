import pandas as pd
from crawler.douban import DoubanSpider
import jieba
import jieba.analyse
from pymongo import MongoClient


class CreateProfile(object):
    def __init__(self, reader):
        self.user_id: int = reader
        self.pos = ['n', 'nz', 'v', 'vd', 'vn', 'l', 'a', 'd', 'tag']
        self.user_profile = {}
        self.users_df = pd.read_excel("./doc/借阅列表测试集.xlsx", skiprows=3, usecols="C:N")
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client['seaeverit']

    def __del__(self):
        self.client.close()

    def get_borrow_history(self, n=30):
        """获取用户借阅历史一年内最近30条书目ISBN"""
        user_df = self.users_df[self.users_df['READERNO'] == self.user_id]
        # 获取最近30篇用户借阅条目
        user_df = user_df.sort_values(by=['EVENTDATE'], ascending=False).iloc[:n, :]
        history_list = []
        for index, row in user_df.iterrows():
            if ';' in row['ISBN']:
                history_list.extend(row['ISBN'].split('; '))
            else:
                history_list.append(row['ISBN'])
        return history_list

    def crawl_books_info(self, isbn_list):
        """通过爬虫爬取用户借阅过的图书信息"""
        spider = DoubanSpider()
        collection = self.db['borrow_books']
        for isbn in isbn_list:
            book_info = spider.get_book_info(isbn)
            book_info["ISBN"] = isbn
            collection.update_one({"ISBN": isbn}, {"$set": book_info}, upsert=True)

    def get_user_preference(self, history_list):
        """根据用户借阅历史获取用户偏好"""
        books_info = []
        for isbn in history_list:
            collection = self.db['borrow_books']
            book_info = collection.find_one({'ISBN': isbn, '豆瓣标题': {'$exists': 1}},
                                            {'_id': 0, 'ISBN': 1, '豆瓣标题': 1, '内容简介': 1, '豆瓣标签': 1})
            if book_info is None or book_info == {}:
                print("数据库中没有该书数据", isbn)
                continue
            else:
                books_info.append(book_info)

        text_list = []
        for info in books_info:
            text_list.append(info['豆瓣标题'] + ', '.join(info['内容简介'] + info['豆瓣标签']))
            # 动态添加豆瓣标签为分词词汇
            for tag in info['豆瓣标签']:
                jieba.add_word(tag, freq=10, tag='tag')
        text = ' '.join(text_list)
        return jieba.analyse.extract_tags(text, topK=20, withWeight=True, allowPOS=self.pos)

    def store_user_to_mongodb(self, borrow_records, preference):
        """将获取的用户信息存入mongodb"""
        user_dict = {
            "reader_id": self.user_id,
            'borrow_records': borrow_records,
            'preference': preference
        }
        collection = self.db['reader']
        collection.update_one({"reader_id": self.user_id}, {"$set": user_dict}, upsert=True)

    def create(self):
        """构建用户画像"""
        history_list = self.get_borrow_history(n=30)
        collection = self.db['borrow_books']
        cursor = collection.find({}, {'_id': 0, 'ISBN': 1})
        borrowed_books = set(item['ISBN'] for item in cursor)
        crawl_list = []
        for isbn in history_list:
            if isbn in borrowed_books:
                continue
            else:
                crawl_list.append(isbn)
        # 爬取书籍数据存入数据库
        print("正在爬取数据")
        self.crawl_books_info(crawl_list)
        print("爬取数据完成")
        preference = self.get_user_preference(history_list)
        self.store_user_to_mongodb(history_list, preference)


if __name__ == "__main__":
    # reader = ['52522','456045', , '119062', '133758', '137939', '138187', '401848', '443741', '363469']
    readers = ['71628']
    for r in readers:
        profile = CreateProfile(int(r))
        profile.create()
