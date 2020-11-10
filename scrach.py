from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


def excel2mysql():
    df = pd.read_excel('./doc/3000读者近5年借阅列表.xlsx')

    # iloc选择行列，to_dict转换成以列标签作为数据key，每一行数据作为一项的字典
    data_list = df.iloc[1:2000, :].to_dict(orient='records')

    # 将序号从1起始，原来数据是从2开始
    for data in data_list:
        data["序号"] -= 1

    # 连接Mysql
    database = 'bookrecommender'
    # create_engine("数据库类型+数据库驱动://数据库用户名:数据库密码@IP地址:端口/数据库"，其他参数)
    engine = create_engine('mysql+mysqldb://root:password@localhost:3306/'
                           '{}?charset=utf8'.format(database))
    df.to_sql(con=engine, name='user', if_exists='replace', flavor='mysql', index=False)
    with engine.connect() as con:
        con.execute("ALTER TABLE `user` ADD PRIMARY KEY (`id`)")
    # 连接mongodb
    with MongoClient("mongodb://localhost:27017") as client:
        collection = client["seaeverit"]["user"]
    collection.insert_many(data_list)


def add_feature_to_mongo(features):
    with MongoClient("mongodb://localhost:27017") as client:
        collection = client['seaeverit']['book_train']
        cursor = collection.find({})
        for i, item in enumerate(cursor):
            collection.update_one({'_id': item['_id']}, {'$set': {'分词': features[i]}})


def transfer_reader_books():
    with MongoClient("mongodb://localhost:27017") as client:
        reader = client['seaeverit']['reader']
        reader_books = client['seaeverit']['borrow_books']
        readers_cursor = reader.find({})
        for reader in readers_cursor:
            for isbn, info in reader['borrow_records'].items():
                info.update({'ISBN': isbn})
                reader_books.update_one({'ISBN': isbn}, {'$set': info}, upsert=True)


if __name__ == '__main__':
    with MongoClient("mongodb://localhost:27017") as client:
        collection = client['seaeverit']['reader']
        q = {"reader_id": "456045"}
        cursor = collection.find(q)
        cursor[0]['preference']

