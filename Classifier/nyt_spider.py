import requests, pymongo, json, time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

api_key_path = "config/NYT_API.key.txt"
with open(api_key_path, 'r') as handle:
    API_KEY = handle.read()
NYT_URL = 'http://api.nytimes.com/svc/search/v2/articlesearch.json'



def call_api(url, payload, p=0):
    payload['page'] = p
    return requests.get(url, params=payload)


def get_response(r):
    raw = json.loads(r.text)
    return raw['response']['meta'], raw['response']['docs']


def get_soup(url):

    agent = 'Project11'
    agent += 'mvabhinav1408@gmail.com'
    headers = {'user_agent': agent}

    try:
        r = requests.get(url, headers=headers)
    except:
        return None

    if r.status_code != 200: return None

    return BeautifulSoup(r.text.encode('utf-8'), features="lxml")


def get_body_text(docs):

    result = []
    for d in docs:
        doc = d.copy()
        if not doc['web_url']:
            continue
        soup = get_soup(doc['web_url'])
        if not soup:
            continue
        body = soup.find_all('div', class_="StoryBodyCompanionColumn")
        if not body:
            continue
        doc['body'] = '\n'.join([x.get_text() for x in body])

        print(doc['web_url'])
        result.append(doc)

    return result


def remove_previously_scraped(coll, docs):

    new_docs = []
    for doc in docs:
        if not articles.count_documents(filter={'_id': doc['_id']}) > 0:
            new_docs.append(doc)

    if new_docs == []:
        return None

    return new_docs


def get_end_date(dt):
    # String-ify the datetime object to YYYMMDD
    yr = str(dt.year)
    mon = '0' * (2 - len(str(dt.month))) + str(dt.month)
    day = '0' * (2 - len(str(dt.day))) + str(dt.day)
    return yr + mon + day


def scrape_articles(coll, last_date):
    page = 0
    while page <= 300:
        print('Page:', page)
        # Request all of the newest articles matching the search term
        payload = {'sort': 'newest',
                   'end_date': get_end_date(last_date),
                   'api-key': API_KEY
                   }
        r = call_api(NYT_URL, payload, page)
        page += 1
        if r.status_code != 200:
            page = 0
            last_date += relativedelta(days=-1)
            print('End Date:', get_end_date(last_date))
            print(r.status_code)
            time.sleep(2)
            continue
        meta, docs = get_response(r)

        new_docs = remove_previously_scraped(coll, docs)
        if not new_docs: continue
        docs_with_body = get_body_text(new_docs)

        for doc in docs_with_body:
            try:
                coll.insert_one(doc)
            except:
                continue



if __name__ == "__main__":
    client = pymongo.MongoClient()
    db = client.fake_news
    articles = db.nyt_articles
    last_date = datetime.now() + relativedelta(days=-1)
    print(last_date)
    scrape_articles(articles, last_date)
    