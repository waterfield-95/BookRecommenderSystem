from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


def migrate_train_data():
    """
    从豆瓣爬取的数据中，获取内容简介，目录，标签信息完整的图书
    """
    client = MongoClient("mongodb://localhost:27017")
    database = client["seaeverit"]
    orig_col = database["book_douban"]
    dest_col = database["book_train"]

    query = {
        "豆瓣-内容简介": {"$exists": True},
        "豆瓣-目录": {"$exists": True},
        "豆瓣-评分": {"$ne": ""},
        "豆瓣-豆瓣成员常用的标签": {"$exists": True}
    }
    projection = {
        "标题": 1,
        "作者": 1,
        "ISBN": 1,
        "豆瓣-内容简介": 1,
        "豆瓣-目录": 1,
        "豆瓣-评分": 1,
        "豆瓣-豆瓣成员常用的标签": 1
    }

    cursor = orig_col.find(query, projection=projection)

    try:
        for doc in cursor:
            # 选取内容，不为空并且描述信息充分
            content = doc["豆瓣-内容简介"]
            if len(content)==0 or (len(content)<2 and len(content[0])<20):
                continue
            # 选取目录
            catalog = doc["豆瓣-目录"]
            if len(catalog)==0 or len(catalog)<3:
                continue
            # 选取标签
            tag = doc["豆瓣-豆瓣成员常用的标签"]
            if len(tag) == 0:
                continue

            dest_col.insert_one(doc)

    finally:
        client.close()


def generate_words_file(frequence=10, pos='tag'):
    """生成豆瓣标签词汇文件，用于扩充分词词表

    文件类型为txt，每一行表示一个词汇: word frequence pos
    """
    with MongoClient("mongodb://localhost:27017") as client:
        database = client["seaeverit"]
        collection = database["book_train"]
        cursor = collection.find({}, {"_id": 0, "豆瓣-豆瓣成员常用的标签": 1})
    tags = []
    for item in cursor:
        tags += item["豆瓣-豆瓣成员常用的标签"]
    with open("douban_tag_dict.txt", "w") as outfile:
        for tag in tags:
            outfile.write("{} {} {}\n".format(tag, 10, "tag"))
    return


def merge_stopwords():
    """将四个stopwords.txt去重合并
    """
    stopwords_list = ["cn", 'baidu', 'scu', 'hit']
    stopwords = pd.DataFrame(columns=["word"])
    for item in stopwords_list:
        s = pd.read_csv("stopwords/{}_stopwords.txt".format(item), header=None, names=["word"], encoding="utf-8")
        stopwords = stopwords.append(s).drop_duplicates()
    stopwords.to_csv("stopwords.txt", sep=" ", index=False, header=False)
    return


def add_url_to_traindb():
    """将URL添加到训练集"""
    with MongoClient("mongodb://localhost:27017") as client:
        douban = client["seaeverit"]["book_douban"]
        train = client["seaeverit"]["book_train"]
        train_cursor = train.find({})
        isbn_list = []
        for item in train_cursor:
            isbn_list.append(item["ISBN"])
        douban_cursor = douban.find({'ISBN': {"$in": isbn_list}})
        for item in douban_cursor:
            train.update_one({"ISBN": item["ISBN"]}, {"$set": {"豆瓣URL": item["豆瓣-URL"]}})


def merge_jd_to_train():
    "将京东数据合并入训练集"
    with MongoClient("mongodb://localhost:27017") as client:
        jd = client["seaeverit"]["book_jd"]
        train = client["seaeverit"]["book_train"]
        train_cursor = train.find({})
        isbn_list = [item["ISBN"] for item in train_cursor]
        projection = {
            '_id': 0, 'ISBN': 1, '京东-URL': 1, '京东-内容简介': 1, '京东-精彩书摘': 1, '京东-精彩书评': 1, '京东-编辑推荐': 1
        }
        jd_cursor = jd.find({'ISBN': {"$in": isbn_list}}, projection)
        for item in jd_cursor:
            train.update_one({"ISBN": item["ISBN"]}, {"$set": item})


if __name__ == "__main__":
    merge_jd_to_train()
