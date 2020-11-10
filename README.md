# Book_recommender_system

## 2020.11.10 CBRecommender

Collie Recommend System, there are four part, which respectively are data crawling from web, feature extraction from books, feature extraction from users and  similarity matching.

### 1. data crawling
- crawl book content description from douban and jd through python (requests, xpath, selenium, redis)
- clean data and store it in mongodb

## 2/3. feature extraction
- word cutting (python module -> jieba)
- tf-idf algorithm to get word weight (sklearn word2vec)
- regard topN keywords as the book feature
- Iterate books from user borrowing records and complete the overall feature extraction, which is regarded as user feature

## 4. similarity matching
- jarccard similarity to compare user with books in books library
- Synonym substitution (python module: synonym)
