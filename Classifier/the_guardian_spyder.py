import requests, pymongo, json, time
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

MY_API_KEY = open("config/THE_GUARDIAN.key.txt").read().strip()
API_ENDPOINT = 'http://content.guardianapis.com/search'
PAGE_SIZE = 50

def call_api(url, payload):
    return requests.get(url, params=payload)


def get_response(r):
    raw = json.loads(r.text)
    return raw


def get_soup(url):
    # Header to be passed in to NYT when scraping article text.
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
    # Grab the url from each document, if it exists, then scrape each url for
    # its body text. If we get any errors along the way, continue on to the
    # next document / url to be scraped.
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
    # Check to see if the mongo collection already contains the docs returned
    # from NYT. Return back a list of the ones that aren't in the collection to
    # be scraped.
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


def scrape_articles(coll, psize=20):
    # Request all of the newest articles matching the search term
    start_date = date(2019, 6,1)
    end_date = date(2019,11,1)
    dayrange = range((end_date - start_date).days + 1)
    for daycount in dayrange:
        dt = start_date + timedelta(days=daycount)
        print(dt)
        dstr = dt.strftime('%Y-%m-%d')
        payload = {
            'from-date': dstr,
            'to-date': dstr,
            'order-by': "newest",
            'show-fields': 'all',
            'page-size': psize,
            'api-key': MY_API_KEY
        }

        r = call_api(API_ENDPOINT, payload)

        if r.status_code != 200:
            return "Fail {}".format(r.status_code)
        docs = get_response(r)
        for doc in docs['response']['results']:
            try:
                coll.insert_one(doc)
            except:
                continue


if __name__ == "__main__":
    client = pymongo.MongoClient()
    db = client.fake_news
    articles = db.tg_articles
    last_date = datetime.now() + relativedelta(days=-2)
    scrape_articles(articles, PAGE_SIZE)
