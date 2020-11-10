from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import synonyms
import json


class ContentBasedRecommend(object):
    def __init__(self, reader_id: int):
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client['seaeverit']
        # [0]返回字典
        self.user_profile = self.db['reader'].find({'reader_id': int(reader_id)})[0]
        self.reader_id = self.user_profile['reader_id']
        self.reader_preference = self.user_profile['preference']
        self.recommender = []

    def __del__(self):
        self.client.close()

    def get_user_feature(self) -> list:
        """获取用户偏好特征：20个关键词表征"""
        return self.user_profile['preference']

    @staticmethod
    def calc_jaccard_similarity(origin_str, match_str, similarity_coefficient=1):
        s1 = set(origin_str.split())
        s2 = set(match_str.split())
        intersection = s1.intersection(s2)
        similarity = similarity_coefficient * float(len(intersection)) / (len(s1) + len(s2) - len(intersection))
        return similarity, list(intersection)

    def recommend(self):
        """获取reader最相似的10本图书"""
        books = pd.read_csv("./data/613_keywords_tfidf_20_1109.csv", skiprows=1, names=["title", "ISBN", "keyword"])
        # (similarity, [共有词汇])
        J_similarity = []
        for index, row in books.iterrows():
            row['ISBN'] = row['ISBN'].replace('-', '')
            # 同义词替换
            keyword_sub = [item[0] for item in eval(row['keyword'])]
            # pref为列表 ['言情', 0.20400812255161557]
            coef = 1
            for pref in self.user_profile['preference']:
                syn_word, syn_sim = synonyms.nearby(pref[0], 20)
                sim_words = [(word, syn_sim[i]) for i, word in enumerate(syn_word) if syn_sim[i] > 0.7]
                final_syn_words = [item[0] for item in sim_words]
                # for item in sim_words:
                #     coef *= item[1]
                for word in keyword_sub:
                    if word in final_syn_words and word in keyword_sub:
                        keyword_sub.remove(word)
                        keyword_sub.append(pref[0])
            similarity, sim_words = self.calc_jaccard_similarity(
                ' '.join([item[0] for item in self.user_profile['preference']]), ' '.join(keyword_sub), coef)
            J_similarity.append((similarity, sim_words))
        similarity_df = pd.DataFrame(J_similarity, columns=['J_similarity', 'sim_words'])
        word_similarity = pd.concat([similarity_df, books], axis=1)
        word_similarity_sorted = word_similarity.sort_values(by="J_similarity", ascending=False)
        # 打印出相似书籍排序
        outfile_name = "./data/{reader}_Jsimilarity_{date}.csv".format(
            reader=self.reader_id, date=datetime.now().strftime('%m%d_%H'))
        res_df = word_similarity_sorted.head(10).loc[:, ['title', 'ISBN', 'J_similarity', 'sim_words']]
        self.store_in_mongo(res_df)
        res_df.to_csv(outfile_name, index=False)
        return res_df

    def store_in_mongo(self, df):
        data = df.to_dict(orient='records')
        collection = self.db['reader']
        collection.update_one({'reader_id': self.reader_id}, {'$set': {'recommend': data}})
        print('已存入MongoDB')


if __name__ == '__main__':
    readers = [71628, 119062, 133758, 137939, 138187, 401848, 443741, 363469]
    # reader = 456045
    for reader in readers:
        rec = ContentBasedRecommend(reader)
        print(rec.recommend())
