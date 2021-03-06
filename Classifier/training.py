import os
import re
import sys
import numpy as np
from scipy.sparse import hstack
import time
import pandas as pd
from nltk.stem.snowball import SnowballStemmer
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, accuracy_score , recall_score , precision_score
import lightgbm as lgb
import wordbatch
from wordbatch.extractors import WordBag, WordHash
from nltk.corpus import stopwords
import pickle as pkl
import gzip

stemmer = SnowballStemmer("english")

stopwords = {x: 1 for x in stopwords.words('english')}
alphanums = re.compile(u'[^A-Za-z0-9]+')

def normalize_text(text):
    return u" ".join(
        [x for x in [y for y in alphanums.sub(' ', text).lower().strip().split(" ")] \
         if len(x) > 1 and x not in stopwords])

def preprocessing(df, kaggle_test_df):

    print("Filling NaNs")
    kaggle_test_df['author'].fillna('No author', inplace=True)
    kaggle_test_df['title'].fillna('No title', inplace=True)
    kaggle_test_df['text'].fillna('No text', inplace=True)

    df.drop('label', axis=1, inplace=True)
    kaggle_test_df.drop('id', axis=1, inplace=True)
    df_full = df.append(kaggle_test_df)

    print("Start Encoding Labels for Author")
    le = LabelEncoder()
    df_full['author_cat'] = le.fit_transform(df_full['author'])


    print("Saving Author | Author Category for later use")
    df_full[['author', 'author_cat']].to_csv('data/author_cat.csv', index=False)

    print("Start Stemming Title")
    df_full['stemmed_title'] = df_full['title'].map(lambda x: ' '.join([stemmer.stem(y) for y in x.split(' ')]))

    print("Start Stemming News Text")
    df_full['stemmed_text'] = df_full['text'].map(lambda x: ' '.join([stemmer.stem(y) for y in x.split(' ')]))
    df_full.drop(['title', 'author', 'text'], axis=1, inplace=True)
    print("Saving the dataset stemmed for later use")
    df_full.to_csv('data/df_stemmed_kaggle.csv', index=False)

    return df_full

def train_lgb(d_train, watchlist, valid_X, valid_y):
    params = {
        'objective': 'binary',
        'learning_rate': 0.1,
        'num_leaves': 31,
        'feature_fraction': 0.65,
        'bagging_fraction': 0.8,
        'bagging_freq': 1,
        'boosting_type': 'gbdt',
        'metric': 'binary_logloss',
        'max_depth': 9,
        'is_unbalance': True
    }

    model = lgb.train(params, train_set=d_train, num_boost_round=6000, valid_sets=watchlist,
                      early_stopping_rounds=200, verbose_eval=1)

    preds = model.predict(valid_X)
    print("LGB dev f1_score:", f1_score(valid_y, np.round(preds)))
    print("LGB dev accuracy_score:", accuracy_score(valid_y, np.round(preds)))
    print("LGB dev recall_score:", recall_score(valid_y, np.round(preds)))
    print("LGB dev precision_score:", precision_score(valid_y, np.round(preds)))

    print('Saving model...')
    if not os.path.exists('data/models/'):
        os.makedirs('data/models/')

    model.save_model('data/models/lgb_model_best_iter.txt', num_iteration=model.best_iteration)
    print('Model Saved')

    return model


if __name__ == '__main__':

    start_time = time.time()

    df_path = 'data/df_stemmed_kaggle.csv'

    print("Start Loading the datasets")
    df = pd.read_csv('data/df_final_v1.csv')
    kaggle_test_df = pd.read_csv('data/kaggle/test.csv')

    train_size = df.shape[0]
    y = df['label']

    test_ids = kaggle_test_df['id']
    test_size = kaggle_test_df.shape[0]

    if os.path.isfile(df_path):
        print('Preprocessed file found! Loading preprocessed Dataset')
        df_full = pd.read_csv(df_path)
    else:
        print('No preprocessed file found, start preprocessing')
        df_full = preprocessing(df, kaggle_test_df)

    wb = wordbatch.WordBatch(normalize_text
                             , extractor=(WordBag, {"hash_ngrams": 2,
                                                    "hash_ngrams_weights": [0.5, -1.0],
                                                    "hash_size": 2 ** 23,
                                                    "norm": 'l2',
                                                    "tf": 'log',
                                                    "idf": 10.0}
                                          )
                             , procs=8)

    wb.dictionary_freeze = True

    X_title = wb.transform(df_full['stemmed_title'])
    print("Xtitle shape", X_title.shape)
    X_text = wb.transform(df_full['stemmed_text'])
    print("X_text shape", X_text.shape)
    X_author = df_full['author_cat'].values
    X_author = X_author.reshape(-1, 1)

    sparse_merge = hstack((X_title, X_text, X_author)).tocsr()

    print("sparse_merge shape", sparse_merge.shape)

    X = sparse_merge[:train_size]
    X_test = sparse_merge[train_size:]

    train_X, valid_X, train_y, valid_y = train_test_split(X, y, test_size=0.05, random_state=100)

    d_train = lgb.Dataset(train_X, label=train_y)
    d_valid = lgb.Dataset(valid_X, label=valid_y)
    watchlist = [d_train, d_valid]

    print("Start Training")
    clf = train_lgb(d_train, watchlist, valid_X, valid_y)

    print("Saving models")
    with gzip.open('data/models/wb_transform.pkl', 'wb') as model_file:
        pkl.dump(wb, model_file, protocol=2)

    print("Models Saved")
