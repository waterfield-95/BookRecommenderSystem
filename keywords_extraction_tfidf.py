from jieba.analyse import tfidf
import jieba.analyse
import jieba.posseg
import pandas as pd
import numpy as np
import sys, codecs
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer, TfidfVectorizer
from pymongo import MongoClient
import time
import re


def data_preprocessing(text, stopwords):
    """将文本进行分词，过滤，输出词汇列表
    """
    # 定义选取的词性: n普通名词，nz其他专名，v普通动词，vd动副词，vn动名词，l习用语，a形容词，d副词，tag豆瓣
    pos = ['n', 'nz', 'v', 'vd', 'vn', 'l', 'a', 'd', 'tag']
    # 精准模式分词
    seg = jieba.posseg.cut(text)

    words = []
    for item in seg:
        # 只保留中文字符（中文字符unicode范围:[\u4e00-\u9fa5]）
        item.word = re.sub(r'[^\u4e00-\u9fa5]', '', item.word)
        if item.word == '':
            continue
        # 去停用词和词性筛选
        if item.word not in stopwords and item.flag in pos:
            words.append(item.word)
    return words


def get_keyword_tfidf(data, topk=10):
    """获取文本top10关键词
    """
    # 导入豆瓣tag作为新词，包括作者名称等
    jieba.load_userdict("douban_tag_dict.txt")

    # 导入停词表
    stopwords_list = []
    with open("stopwords.txt", "r") as f:
        for line in f.readlines():
            stopwords_list.append(line.replace('\n', ''))

    # jieba.enable_parallel(4) # 没有改变分词时间

    # 将所有文档输出到一个list中，一行就是一个文档；单进程613file：21s
    corpus = []
    t1 = time.time()
    for sentence in data['info']:
        corpus.append(" ".join(data_preprocessing(sentence, stopwords_list)))
    t2 = time.time()
    print("Cut words time consuming: ", t2-t1)

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
    for i in range(len(weight)):
        # 当前文章的所有词汇列表、词汇对应权重列表
        word_list, weight_list = [], []
        for j in range(len(words)):
            word_list.append(words[j])
            weight_list.append(weight[i][j])

        df_word = pd.DataFrame(word_list, columns=['word'])
        df_weight = pd.DataFrame(weight_list, columns=['weight'])
        word_weight = pd.concat([df_word, df_weight], axis=1)   # 列拼接词表和词tfidf
        word_weight_sorted = word_weight.sort_values(by="weight", ascending=False)    # 按照weight降序排列

        keyword = np.array(word_weight_sorted['word'])
        top_words = [keyword[i] for i in range(topk)]
        top_string = " ".join(top_words)
        keywords.append(top_string)

    df_keywords = pd.DataFrame({"keywords": keywords})
    res = pd.concat([data[['title', 'ISBN']], df_keywords], axis=1)
    return res


def get_data():
    """从MongoDB中获取书籍数据

    处理为DataFrame(columns=["title", "ISBN", "info"])输出
    """
    client = MongoClient("mongodb://localhost:27017")
    database = client["seaeverit"]
    collection = database["book_train_1"]
    projection = {
        '_id': 0,
        "标题": 1,
        "ISBN": 1,
        "豆瓣-内容简介": 1,
        "豆瓣-目录": 1,
        "豆瓣-豆瓣成员常用的标签": 1
    }
    data_dict = collection.find({}, projection=projection)
    df = pd.DataFrame.from_records(data_dict)
    for index, row in df.iterrows():
        info = ", ".join([row["标题"]] + row["豆瓣-内容简介"] + row["豆瓣-目录"] + row["豆瓣-豆瓣成员常用的标签"])
        df.at[index, "info"] = info
    df.rename(columns={"标题": "title"}, inplace=True)
    res_df = df.loc[:, ["title", "ISBN", "info"]]
    return res_df


def generate_words_file(frequence=10, pos='tag'):
    """生成豆瓣标签词汇文件，用于扩充分词词表

    文件类型为txt，每一行表示一个词汇: word frequence pos
    """
    client = MongoClient("mongodb://localhost:27017")
    database = client["seaeverit"]
    collection = database["book_train_1"]
    cursor = collection.find({}, {"_id": 0, "豆瓣-豆瓣成员常用的标签": 1})
    tags = []
    for item in cursor:
        tags += item["豆瓣-豆瓣成员常用的标签"]
    with open("douban_tag_dict.txt", "w") as outfile:
        for tag in tags:
            outfile.write("{} {} {}\n".format(tag, 10, "tag"))


def merge_stopwords():
    stopwords_list = ["cn", 'baidu', 'scu', 'hit']
    stopwords = pd.DataFrame(columns=["word"])
    # stopwords = pd.read_csv("stopwords/cn_stopwords.txt", header=None, names=["word"])
    for item in stopwords_list:
        s = pd.read_csv("stopwords/{}_stopwords.txt".format(item), header=None, names=["word"], encoding="utf-8")
        stopwords = stopwords.append(s).drop_duplicates()
    stopwords.to_csv("stopwords.txt", sep=" ", index=False, header=False)


def get_jaccard_similarity(str1, str2):
    s1 = set(str1.split())
    s2 = set(str2.split())
    intersection  = s1.intersection(s2)
    jaccard = float(len(intersection)) / (len(s1) + len(s2) - len(intersection))
    return jaccard


from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def get_cosine_sim(*strs):
    vectors = [t for t in get_vectors(*strs)]
    return cosine_similarity(vectors)


def get_vectors(*strs):
    text = [t for t in strs]
    vectorizer = CountVectorizer(text)
    vectorizer.fit(text)
    return vectorizer.transform(text).toarray()


def main():
    data = get_data()
    df_keywords = get_keyword_tfidf(data, topk=10)
    df_keywords.to_csv("./data/613_keywords_tfidf_filter10.csv", sep=',')


if __name__ == "__main__":
    main()
