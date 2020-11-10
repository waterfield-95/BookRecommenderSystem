import jieba.analyse
import jieba.posseg
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
import time
import re
from datetime import datetime


class KeywordExtraction(object):
    def __init__(self, topk=10):
        self.topk = topk
        self.stopwords = []
        # 定义选取的词性: n普通名词，nz其他专名，v普通动词，vd动副词，vn动名词，l习用语，a形容词，d副词，tag豆瓣
        self.pos = ['n', 'nz', 'v', 'vd', 'vn', 'l', 'a', 'd', 'tag']
        # 导入豆瓣tag作为新词，包括作者名称等
        jieba.load_userdict("douban_tag_dict.txt")

    def _get_stopwords(self):
        """导入停词表"""
        with open("stopwords.txt", "r") as f:
            for line in f.readlines():
                self.stopwords.append(line.replace('\n', ''))

    @staticmethod
    def get_data():
        """从MongoDB中获取书籍数据

        处理为DataFrame(columns=["title", "ISBN", "info"])输出
        """
        with MongoClient("mongodb://localhost:27017") as client:
            database = client["seaeverit"]
            collection = database["book_train"]
            projection = {
                '_id': 0,
                "标题": 1,
                "ISBN": 1,
                "豆瓣-内容简介": 1,
                "豆瓣-豆瓣成员常用的标签": 1,
                # "豆瓣评论": 1,
                "京东-内容简介": 1,
                "京东-精彩书摘": 1,
                "京东-精彩书评": 1,
                "京东-编辑推荐": 1,
            }
            data_dict = collection.find({}, projection=projection)
        df = pd.DataFrame.from_records(data_dict)
        l = ["豆瓣-内容简介", "豆瓣-豆瓣成员常用的标签", "京东-内容简介", "京东-精彩书摘", "京东-精彩书评", "京东-编辑推荐"]
        for index, row in df.iterrows():
            full_text = row["标题"]
            for item in l:
                if isinstance(row[item], float):
                    continue
                else:
                    full_text += ", ".join(row[item])
            df.at[index, "info"] = full_text
        df.rename(columns={"标题": "title"}, inplace=True)
        return df.loc[:, ["title", "ISBN", "info"]]

    def data_preprocessing(self, text):
        """将文本进行分词，过滤，输出词汇列表
        """
        seg = jieba.posseg.cut(text)    # 精准模式分词
        words = []
        for item in seg:
            # 只保留中文字符（中文字符unicode范围:[\u4e00-\u9fa5]）
            item.word = re.sub(r'[^\u4e00-\u9fa5]', '', item.word)
            if item.word == '':
                continue
            # 去停用词和词性筛选
            if item.word not in self.stopwords and item.flag in self.pos:
                words.append(item.word)
        return words

    def get_keyword_tfidf(self, data: pd.DataFrame) -> pd.DataFrame:
        """获取文本top10关键词

        data['info'] 用于分词的字符串，其他列是书籍其他信息
        """
        self._get_stopwords()
        # 分词：将所有文档输出到一个list中，一行就是一个文档；单进程613file：150s
        with MongoClient("mongodb://localhost:27017") as client:
            collection = client['seaeverit']['book_train']
            content = [item for item in collection.find({'分词': {'$exists': True}})]
            if len(content) == 0:  # 未进行分词
                corpus = []
                t1 = time.time()
                for sentence in data['info']:
                    corpus.append(" ".join(self.data_preprocessing(sentence)))
                t2 = time.time()
                print("Cut words time consuming: ", t2-t1)
                cursor = collection.find({})
                for i, item in enumerate(cursor):
                    collection.update_one({'_id': item['_id']}, {'$set': {'分词': corpus[i]}})
            else:   # 已分词，直接数据库读取
                corpus = [item['分词'] for item in collection.find({}, {'分词': 1})]

        # 构建tf-idf矩阵
        vectorizer = TfidfVectorizer()
        # 拟合变换，生成(第几个item，某个词汇在整个文件中出现的次数) 该词在这个item中的tfidf
        tf_idf = vectorizer.fit_transform(corpus)
        # 获取所有词袋法中的特征词，便于观察
        words = vectorizer.get_feature_names()
        # weight[i][j] 表示j词在第i篇文章中的tf-idf权重，越大越重要
        weight = tf_idf.toarray()

        # 遍历排序，找出每本书的keywords
        keywords = []
        items = []
        for i in range(len(weight)):
            # 当前文章的所有词汇列表、词汇对应权重列表
            word_list, weight_list = [], []
            for j in range(len(words)):
                word_list.append(words[j])
                weight_list.append(weight[i][j])
            df_word = pd.DataFrame(word_list, columns=['word'])
            df_weight = pd.DataFrame(weight_list, columns=['weight'])
            # 列拼接词表和词tfidf
            word_weight = pd.concat([df_word, df_weight], axis=1)
            # 按照weight降序排列
            word_weight_sorted = word_weight.sort_values(by="weight", ascending=False)
            keyword = np.array(word_weight_sorted['word'])
            keyword_weight = np.array(word_weight_sorted['weight'])
            # top_words = [keyword[i] for i in range(self.topk)]
            # top_string = " ".join(top_words)
            # keywords.append(top_string)
            # 包含权重的关键词
            top_items = [(keyword[i], keyword_weight[i]) for i in range(self.topk)]
            items.append(top_items)
        # df_keywords = pd.DataFrame({"keywords": keywords})
        # df_keywords = pd.concat([data[['title', 'ISBN']], df_keywords], axis=1)
        df_items = pd.DataFrame({'weight': items})
        df_items = pd.concat([data.loc[:, ['title', 'ISBN']], df_items], axis=1)
        return df_items

    def main(self):
        data = self.get_data()
        df_keywords = self.get_keyword_tfidf(data)
        df_keywords.to_csv("./data/{}".format('613_keywords_tfidf_20_{}.csv'.format(
            datetime.now().strftime('%m%d'))), sep=',')
        with MongoClient("mongodb://localhost:27017") as client:
            collection = client['seaeverit']['book_train']
            for i, row in df_keywords.iterrows():
                collection.update_one({'ISBN': row['ISBN']}, {'$set': {'keywords': row['weight']}})


if __name__ == "__main__":
    extraction = KeywordExtraction(topk=20)
    extraction.main()
