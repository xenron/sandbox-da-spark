# AN - 20150602
#
#
# 0- IO - CSV File: Save and Load
# 1- IO - JSON File: Save and Load
# 2- IO - MONGO DB: Connect, Save and Load
# 3- IO - odo exchange data (Pandas Dataframe)
# 4- Query - Blaze
# 5- Query - SparkSQL
# 6- Twitter API call - robust calls taking into account rate limit
​
In [1]:
# imports
# import unicode
import os
import io
import json
import pymongo
from pprint import pprint as pp
import csv
from collections import namedtuple
import time
In [2]:
class IO_json(object):
    def __init__(self, filepath, filename, filesuffix='json'):
        self.filepath = filepath        # /path/to/file  without the '/' at the end
        self.filename = filename        # FILE_NAME
        self.filesuffix = filesuffix
        # self.file_io = os.path.join(dir_name, '.'.join((base_filename, filename_suffix)))
​
    def save(self, data):
        if os.path.isfile('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix)):
            # Append existing file
            with io.open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'a', encoding='utf-8') as f:
                f.write(unicode(json.dumps(data, ensure_ascii= False))) # In python 3, there is no "unicode" function 
                # f.write(json.dumps(data, ensure_ascii= False)) # create a \" escape char for " in the saved file        
        else:
            # Create new file
            with io.open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'w', encoding='utf-8') as f:
                f.write(unicode(json.dumps(data, ensure_ascii= False)))
                # f.write(json.dumps(data, ensure_ascii= False))    
​
    def load(self):
        with io.open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), encoding='utf-8') as f:
            return f.read()
In [43]:
class IO_csv(object):
    def __init__(self, filepath, filename, filesuffix='csv'):
        self.filepath = filepath       # /path/to/file  without the '/' at the end
        self.filename = filename       # FILE_NAME
        self.filesuffix = filesuffix
        # self.file_io = os.path.join(dir_name, '.'.join((base_filename, filename_suffix)))
​
    def save(self, data, NTname, fields):
        # NTname = Name of the NamedTuple
        # fields = header of CSV - list of the fields name
        NTuple = namedtuple(NTname, fields)
        
        if os.path.isfile('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix)):
            # Append existing file
            with open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'ab') as f:
                writer = csv.writer(f)
                # writer.writerow(fields) # fields = header of CSV
                writer.writerows([row for row in map(NTuple._make, data)])
                # list comprehension using map on the NamedTuple._make() iterable and the data file to be saved
                # Notice writer.writerows and not writer.writerow (i.e. list of multiple rows sent to csv file
        else:
            # Create new file
            with open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'wb') as f:
                writer = csv.writer(f)
                writer.writerow(fields) # fields = header of CSV - list of the fields name
                writer.writerows([row for row in map(NTuple._make, data)])
                #  list comprehension using map on the NamedTuple._make() iterable and the data file to be saved
                # Notice writer.writerows and not writer.writerow (i.e. list of multiple rows sent to csv file
            
    def load(self, NTname, fields):
        # NTname = Name of the NamedTuple
        # fields = header of CSV - list of the fields name
        NTuple = namedtuple(NTname, fields)
        with open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix),'rU') as f:
            reader = csv.reader(f)
            for row in map(NTuple._make, reader):
                # Using map on the NamedTuple._make() iterable and the reader file to be loaded
                yield row
In [4]:
​
def parse_date(s):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(s,'%a %b %d %H:%M:%S +0000 %Y'))
​
In [ ]:
def parse_geo(g,index):
    try:
        return str(g["geo"]["coordinates"][index])
    except:
        return ""
In [48]:
def extract_tweet(statuses):
    return [ {'id'         :status['id'], 
              'created_at' :parse_date(status['created_at']), 
              'user_id'    :status['user']['id'],
              'user_name'  :status['user']['name'], 
              'tweet_text' :status['text'].encode('utf-8'), 
              'url'        :url['expanded_url']} 
                               for status in statuses
                                   for url in status['entities']['urls'] ]
In [49]:
def extract_tweet_noURL(statuses):
    return [ {'id'         :status['id'], 
              'created_at' :parse_date(status['created_at']), 
              'user_id'    :status['user']['id'],
              'user_name'  :status['user']['name'], 
              'tweet_text' :status['text'].encode('utf-8') }
                               for status in statuses ]
In [26]:
# from collections import namedtuple
​
fields01 = ['id', 'created_at', 'user_id', 'user_name', 'tweet_text', 'url']
Tweet01 = namedtuple('Tweet01',fields01)
​
​
def parse_tweet(data):
    """
    Parse a ``tweet`` from the given response data.
    """
    return Tweet01(
        id=data.get('id', None),
        created_at=data.get('created_at', None),
        user_id=data.get('user_id', None),
        user_name=data.get('user_name', None),
        tweet_text=data.get('tweet_text', None),
        url=data.get('url')
    )
​
In [25]:
# from collections import namedtuple
​
fields00 = ['id', 'created_at', 'user_id', 'user_name', 'tweet_text']
Tweet00 = namedtuple('Tweet00',fields00)
​
​
def parse_tweet_noURL(data):
    """
    Parse a ``tweet`` from the given response data.
    """
    return Tweet00(
        id=data.get('id', None),
        created_at=data.get('created_at', None),
        user_id=data.get('user_id', None),
        user_name=data.get('user_name', None),
        tweet_text=data.get('tweet_text', None)
    )
​
In [ ]:
import twitter
import urlparse # python 2.7
# import urllib # python 3.0
import logging
import time
from datetime import datetime
​
# from pprint import pprint as pp
​
In [ ]:
​
class TwitterAPI(object):
    """
    TwitterAPI class allows the Connection to Twitter via OAuth
    once you have registered with Twitter and receive the 
    necessary credentials 
    """
​
    def __init__(self): 
        consumer_key = 'get_your_credentials'
        consumer_secret = 'get_your_credentials'
        access_token = 'get_your_credentials'
        access_secret = 'get_your_credentials'
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        self.retries = 3
        self.auth = twitter.oauth.OAuth(access_token, access_secret, consumer_key, consumer_secret)
        self.api = twitter.Twitter(auth=self.auth)
        
        # logger initialisation
        appName = 'twt150530'
        self.logger = logging.getLogger(appName)
        #self.logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        logPath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
        fileName = appName
        fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fileHandler.setFormatter(formatter)
        self.logger.addHandler(fileHandler) 
        self.logger.setLevel(logging.DEBUG)
        
        # Save to JSON file initialisation
        jsonFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
        jsonFname = 'twtr15053001'
        self.jsonSaver = IO_json(jsonFpath, jsonFname)
        
        # Save to MongoDB Intitialisation
        self.mongoSaver = IO_mongo(db='twtr01_db', coll='twtr01_coll')
​
​
    def searchTwitter(self, q, max_res=10,**kwargs):
        search_results = self.api.search.tweets(q=q, count=10, **kwargs)
        statuses = search_results['statuses']
        max_results = min(1000, max_res)
        
        for _ in range(10):
            try:
                next_results = search_results['search_metadata']['next_results']
                # self.logger.info('info in searchTwitter - next_results:%s'% next_results[1:])
            except KeyError as e:
                self.logger.error('error in searchTwitter: %s', %(e))
                break
            
            # next_results = urlparse.parse_qsl(next_results[1:]) # python 2.7
            next_results = urllib.parse.parse_qsl(next_results[1:])
            # self.logger.info('info in searchTwitter - next_results[max_id]:', next_results[0:])
            kwargs = dict(next_results)
            # self.logger.info('info in searchTwitter - next_results[max_id]:%s'% kwargs['max_id'])
            search_results = self.api.search.tweets(**kwargs)
            statuses += search_results['statuses']
            self.saveTweets(search_results['statuses'])
            
            if len(statuses) > max_results:
                self.logger.info('info in searchTwitter - got %i tweets - max: %i' %(len(statuses), max_results))
                break
        return statuses
​
    def saveTweets(self, statuses):
        # Saving to JSON File
        self.jsonSaver.save(statuses)
        
        # Saving to MongoDB
        for s in statuses:
            self.mongoSaver.save(s)
​
    def parseTweets(self, statuses):
        return [ (status['id'], 
                  status['created_at'], 
                  status['user']['id'],
                  status['user']['name'], 
                  status['text'], 
                  url['expanded_url']) 
                        for status in statuses 
                            for url in status['entities']['urls'] ]
​
​
    def getTweets(self, q,  max_res=10):
        """
        Make a Twitter API call whilst managing rate limit and errors.
        """
        def handleError(e, wait_period=2, sleep_when_rate_limited=True):
​
            if wait_period > 3600: # Seconds
                self.logger.error('Too many retries in getTweets: %s', %(e))
                raise e
            if e.e.code == 401:
                self.logger.error('error 401 * Not Authorised * in getTweets: %s', %(e))
                return None
            elif e.e.code == 404:
                self.logger.error('error 404 * Not Found * in getTweets: %s', %(e))
                return None
            elif e.e.code == 429: 
                self.logger.error('error 429 * API Rate Limit Exceeded * in getTweets: %s', %(e))
                if sleep_when_rate_limited:
                    self.logger.error('error 429 * Retrying in 15 minutes * in getTweets: %s', %(e))
                    sys.stderr.flush()
                    time.sleep(60*15 + 5)
                    self.logger.info('error 429 * Retrying now * in getTweets: %s', %(e))
                    return 2
                else:
                    raise e # Caller must handle the rate limiting issue
            elif e.e.code in (500, 502, 503, 504):
                self.logger.info('Encountered %i Error. Retrying in %i seconds' % (e.e.code, wait_period))
                time.sleep(wait_period)
                wait_period *= 1.5
                return wait_period
            else:
                self.logger.error('Exit - aborting - %s', %(e))
                raise e
        
        while True:
            try:
                self.searchTwitter( q, max_res=10)
            except twitter.api.TwitterHTTPError as e:
                error_count = 0 
                wait_period = handleError(e, wait_period)
                if wait_period is None:
                    return
In [ ]:
# test file
fpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
fname = 'twtr15051401'
fsuffix =  'json'
In [8]:
# Check file exist in the path specified
jsonFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
jsonFname = 'twtr15051401'
jsonSuffix = 'json'
os.path.isfile('{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix))
Out[8]:
True
In [9]:
#
# load tweets from file as a text string
#
twts_ld = IO_json(jsonFpath, jsonFname).load()
In [12]:
#
# Convert loaded tweets to Json
#
twts_js = json.loads(twts_ld)
pp(twts_js[1])
​
{u'contributors': None,
 u'coordinates': None,
 u'created_at': u'Thu May 14 12:42:37 +0000 2015',
 u'entities': {u'hashtags': [],
               u'symbols': [],
               u'urls': [],
               u'user_mentions': [{u'id': 3187046084,
                                   u'id_str': u'3187046084',
                                   u'indices': [0, 9],
                                   u'name': u'Spark is Particle',
                                   u'screen_name': u'spark_io'},
                                  {u'id': 487010011,
                                   u'id_str': u'487010011',
                                   u'indices': [17, 26],
                                   u'name': u'Particle',
                                   u'screen_name': u'particle'},
                                  {u'id': 17877351,
                                   u'id_str': u'17877351',
                                   u'indices': [88, 97],
                                   u'name': u'SparkFun Electronics',
                                   u'screen_name': u'sparkfun'},
                                  {u'id': 1551361069,
                                   u'id_str': u'1551361069',
                                   u'indices': [108, 120],
                                   u'name': u'Apache Spark',
                                   u'screen_name': u'ApacheSpark'}]},
 u'favorite_count': 0,
 u'favorited': False,
 u'geo': None,
 u'id': 598830778269769728,
 u'id_str': u'598830778269769728',
 u'in_reply_to_screen_name': u'spark_io',
 u'in_reply_to_status_id': None,
 u'in_reply_to_status_id_str': None,
 u'in_reply_to_user_id': 3187046084,
 u'in_reply_to_user_id_str': u'3187046084',
 u'lang': u'en',
 u'metadata': {u'iso_language_code': u'en', u'result_type': u'recent'},
 u'place': None,
 u'retweet_count': 0,
 u'retweeted': False,
 u'source': u'<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>',
 u'text': u'@spark_io is now @particle - awesome news - now I can enjoy my Particle Cores/Photons + @sparkfun sensors + @ApacheSpark analytics :-)',
 u'truncated': False,
 u'user': {u'contributors_enabled': False,
           u'created_at': u'Mon Aug 25 14:01:26 +0000 2008',
           u'default_profile': True,
           u'default_profile_image': False,
           u'description': u'Building open source tools for and teaching enterprise software developers',
           u'entities': {u'description': {u'urls': []},
                         u'url': {u'urls': [{u'display_url': u'burrsutter.com',
                                             u'expanded_url': u'http://burrsutter.com',
                                             u'indices': [0, 22],
                                             u'url': u'http://t.co/TSHp13EWeu'}]}},
           u'favourites_count': 49,
           u'follow_request_sent': False,
           u'followers_count': 1187,
           u'following': False,
           u'friends_count': 123,
           u'geo_enabled': True,
           u'id': 15981533,
           u'id_str': u'15981533',
           u'is_translation_enabled': False,
           u'is_translator': False,
           u'lang': u'en',
           u'listed_count': 80,
           u'location': u'Raleigh, NC',
           u'name': u'Burr Sutter',
           u'notifications': False,
           u'profile_background_color': u'C0DEED',
           u'profile_background_image_url': u'http://abs.twimg.com/images/themes/theme1/bg.png',
           u'profile_background_image_url_https': u'https://abs.twimg.com/images/themes/theme1/bg.png',
           u'profile_background_tile': False,
           u'profile_image_url': u'http://pbs.twimg.com/profile_images/61514861/TDC6_normal.jpg',
           u'profile_image_url_https': u'https://pbs.twimg.com/profile_images/61514861/TDC6_normal.jpg',
           u'profile_link_color': u'0084B4',
           u'profile_sidebar_border_color': u'C0DEED',
           u'profile_sidebar_fill_color': u'DDEEF6',
           u'profile_text_color': u'333333',
           u'profile_use_background_image': True,
           u'protected': False,
           u'screen_name': u'burrsutter',
           u'statuses_count': 865,
           u'time_zone': u'Eastern Time (US & Canada)',
           u'url': u'http://t.co/TSHp13EWeu',
           u'utc_offset': -14400,
           u'verified': False}}
In [50]:
# Extract key information from Tweets
#
twts_ls_no_url = extract_tweet_noURL(twts_js)
pp(twts_ls_no_url)
[{'created_at': '2015-05-14 12:43:57',
  'id': 598831111406510082,
  'tweet_text': 'RT @pacoid: Great recap of @StrataConf EU in London, with @ApacheSpark summary: DataFrame, R integration, etc. http://t.co/aayCExDbqh',
  'user_id': 14755521,
  'user_name': u'raulsaeztapia'},
 {'created_at': '2015-05-14 12:42:37',
  'id': 598830778269769728,
  'tweet_text': '@spark_io is now @particle - awesome news - now I can enjoy my Particle Cores/Photons + @sparkfun sensors + @ApacheSpark analytics :-)',
  'user_id': 15981533,
  'user_name': u'Burr Sutter'},
 {'created_at': '2015-05-14 12:28:17',
  'id': 598827171168264192,
  'tweet_text': 'RT @baandrzejczak: @rabbitonweb speaking about #ApacheSpark optimization at #GeeCON http://t.co/jaBE0QfL9U',
  'user_id': 20909005,
  'user_name': u'Pawe\u0142 Szulc'},
 {'created_at': '2015-05-14 12:18:36',
  'id': 598824733086523393,
  'tweet_text': '@rabbitonweb speaking about #ApacheSpark optimization at #GeeCON http://t.co/jaBE0QfL9U',
  'user_id': 2787206368,
  'user_name': u'Bartek Andrzejczak'},
 {'created_at': '2015-05-14 11:15:52',
  'id': 598808944719593472,
  'tweet_text': 'RT @alvaroagea: Simply @ApacheSpark http://t.co/gUqUUL8dYz http://t.co/Z782abZ9Dy https://t.co/6itT7lIOjk  #SparkNameContest http://t.co/9h\xe2\x80\xa6',
  'user_id': 14755521,
  'user_name': u'raulsaeztapia'},
 {'created_at': '2015-05-14 10:25:15',
  'id': 598796205091500032,
  'tweet_text': 'RT @PrabhaGana: What exactly is @ApacheSpark and what real-world business problems will it help solve? http://t.co/o6Df0g9Y97 @BlueDataInc \xe2\x80\xa6',
  'user_id': 48695135,
  'user_name': u'John Humphreys'},
 {'created_at': '2015-05-14 09:54:52',
  'id': 598788561127735296,
  'tweet_text': "RT @Ellen_Friedman: I'm still on Euro time. If you are too check out this new Spark tutorial from MapR: http://t.co/YfqhQPtlny Good stuff. \xe2\x80\xa6",
  'user_id': 2385931712,
  'user_name': u"Leonardo D'Ambrosi"},
 {'created_at': '2015-05-14 09:42:53',
  'id': 598785545557438464,
  'tweet_text': "RT @Ellen_Friedman: I'm still on Euro time. If you are too check out this new Spark tutorial from MapR: http://t.co/YfqhQPtlny Good stuff. \xe2\x80\xa6",
  'user_id': 461020977,
  'user_name': u'Alexey Kosenkov'},
 {'created_at': '2015-05-14 09:32:39',
  'id': 598782970082807808,
  'tweet_text': 'RT @BigDataTechCon: Moving Rating Prediction with #ApacheSpark, and Hortonworks http://t.co/urn2hlqfZB #BigData',
  'user_id': 1377652806,
  'user_name': u'embeddedcomputer.nl'},
 {'created_at': '2015-05-14 09:12:38',
  'id': 598777933730160640,
  'tweet_text': "I'm still on Euro time. If you are too check out this new Spark tutorial from MapR: http://t.co/YfqhQPtlny Good stuff. #ApacheSpark #bigdata",
  'user_id': 294862170,
  'user_name': u'Ellen Friedman'}]
In [51]:
len(twts_ls_no_url)
Out[51]:
10
In [52]:
# Extract key information from Tweets
#
twts_ls_url = extract_tweet(twts_js)
pp(twts_ls_url)
[{'created_at': '2015-05-14 12:43:57',
  'id': 598831111406510082,
  'tweet_text': 'RT @pacoid: Great recap of @StrataConf EU in London, with @ApacheSpark summary: DataFrame, R integration, etc. http://t.co/aayCExDbqh',
  'url': u'http://www.mango-solutions.com/wp/2015/05/the-2015-strata-hadoop-world-london/',
  'user_id': 14755521,
  'user_name': u'raulsaeztapia'},
 {'created_at': '2015-05-14 11:15:52',
  'id': 598808944719593472,
  'tweet_text': 'RT @alvaroagea: Simply @ApacheSpark http://t.co/gUqUUL8dYz http://t.co/Z782abZ9Dy https://t.co/6itT7lIOjk  #SparkNameContest http://t.co/9h\xe2\x80\xa6',
  'url': u'http://www.webex.com/ciscospark/',
  'user_id': 14755521,
  'user_name': u'raulsaeztapia'},
 {'created_at': '2015-05-14 11:15:52',
  'id': 598808944719593472,
  'tweet_text': 'RT @alvaroagea: Simply @ApacheSpark http://t.co/gUqUUL8dYz http://t.co/Z782abZ9Dy https://t.co/6itT7lIOjk  #SparkNameContest http://t.co/9h\xe2\x80\xa6',
  'url': u'http://sparkjava.com/',
  'user_id': 14755521,
  'user_name': u'raulsaeztapia'},
 {'created_at': '2015-05-14 11:15:52',
  'id': 598808944719593472,
  'tweet_text': 'RT @alvaroagea: Simply @ApacheSpark http://t.co/gUqUUL8dYz http://t.co/Z782abZ9Dy https://t.co/6itT7lIOjk  #SparkNameContest http://t.co/9h\xe2\x80\xa6',
  'url': u'https://www.sparkfun.com/',
  'user_id': 14755521,
  'user_name': u'raulsaeztapia'},
 {'created_at': '2015-05-14 10:25:15',
  'id': 598796205091500032,
  'tweet_text': 'RT @PrabhaGana: What exactly is @ApacheSpark and what real-world business problems will it help solve? http://t.co/o6Df0g9Y97 @BlueDataInc \xe2\x80\xa6',
  'url': u'http://bit.ly/1JduSUT',
  'user_id': 48695135,
  'user_name': u'John Humphreys'},
 {'created_at': '2015-05-14 09:54:52',
  'id': 598788561127735296,
  'tweet_text': "RT @Ellen_Friedman: I'm still on Euro time. If you are too check out this new Spark tutorial from MapR: http://t.co/YfqhQPtlny Good stuff. \xe2\x80\xa6",
  'url': u'http://bit.ly/1Hfd0Xm',
  'user_id': 2385931712,
  'user_name': u"Leonardo D'Ambrosi"},
 {'created_at': '2015-05-14 09:42:53',
  'id': 598785545557438464,
  'tweet_text': "RT @Ellen_Friedman: I'm still on Euro time. If you are too check out this new Spark tutorial from MapR: http://t.co/YfqhQPtlny Good stuff. \xe2\x80\xa6",
  'url': u'http://bit.ly/1Hfd0Xm',
  'user_id': 461020977,
  'user_name': u'Alexey Kosenkov'},
 {'created_at': '2015-05-14 09:32:39',
  'id': 598782970082807808,
  'tweet_text': 'RT @BigDataTechCon: Moving Rating Prediction with #ApacheSpark, and Hortonworks http://t.co/urn2hlqfZB #BigData',
  'url': u'http://buff.ly/1QBpk8J',
  'user_id': 1377652806,
  'user_name': u'embeddedcomputer.nl'},
 {'created_at': '2015-05-14 09:12:38',
  'id': 598777933730160640,
  'tweet_text': "I'm still on Euro time. If you are too check out this new Spark tutorial from MapR: http://t.co/YfqhQPtlny Good stuff. #ApacheSpark #bigdata",
  'url': u'http://bit.ly/1Hfd0Xm',
  'user_id': 294862170,
  'user_name': u'Ellen Friedman'}]
In [53]:
len(twts_ls_url)
Out[53]:
9
In [54]:
#
# Create list of Tweets NamedTuple (by passing list of tweets through function parseTweets
#
twts_nt_no_url =[parse_tweet_noURL(t) for t in twts_ls_no_url]
# print(parseTweet(t))
print(twts_nt_no_url[1])
Tweet00(id=598830778269769728, created_at='2015-05-14 12:42:37', user_id=15981533, user_name=u'Burr Sutter', tweet_text='@spark_io is now @particle - awesome news - now I can enjoy my Particle Cores/Photons + @sparkfun sensors + @ApacheSpark analytics :-)')
In [55]:
#
# Create list of Tweets NamedTuple (by passing list of tweets through function parseTweets
#
twts_nt_url =[parse_tweet(t) for t in twts_ls_url]
# print(parseTweet(t))
print(twts_nt_url[1])
Tweet01(id=598808944719593472, created_at='2015-05-14 11:15:52', user_id=14755521, user_name=u'raulsaeztapia', tweet_text='RT @alvaroagea: Simply @ApacheSpark http://t.co/gUqUUL8dYz http://t.co/Z782abZ9Dy https://t.co/6itT7lIOjk  #SparkNameContest http://t.co/9h\xe2\x80\xa6', url=u'http://www.webex.com/ciscospark/')
In [56]:
csvFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
csvFname = 'twtr15051401'
csvSuffix = 'csv'
os.path.isfile('{0}/{1}.{2}'.format(csvFpath, csvFname, csvSuffix))
Out[56]:
True
In [57]:
​
# Instantiate the CSV IO object
# Save tweets NamedTuple as a csv
#
twts_csv = IO_csv(csvFpath, csvFname, csvSuffix)
In [58]:
#
# Tweet NamedTuple definitions to be passed to CSV as parameters
#
fields = ['id', 'created_at', 'user_id', 'user_name', 'tweet_text', 'url']
Tweet_NT = 'Tweet01'
​
In [60]:
#
# Executed the save twice - first in csv file create mode follwed by append mode
#
twts_csv.save(twts_nt_url, Tweet_NT, fields)
In [61]:
# read the Tweets CSV 
#
twts_csv_read = [t for t in twts_csv.load(Tweet_NT, fields)]
​
#
#
In [62]:
len(twts_csv_read)
Out[62]:
20
In [63]:
import numpy as np
import pandas as pd
from blaze import Data, by, join, merge
from odo import odo
BokehJS successfully loaded.
In [65]:
# http://stackoverflow.com/questions/20012507/pandas-a-clean-way-to-initialize-data-frame-with-a-list-of-namedtuple
#
twts_pd_df = pd.DataFrame(twts_csv_read, columns=Tweet01._fields)
twts_pd_df.head()
Out[65]:
	id 	created_at 	user_id 	user_name 	tweet_text 	url
0 	id 	created_at 	user_id 	user_name 	tweet_text 	url
1 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
2 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
3 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://www.webex.com/ciscospark/
4 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://sparkjava.com/
In [66]:
# remove first row as it is a duplicate with the header
twts_pd_df = twts_pd_df.drop(twts_pd_df.index[:1])
​
twts_pd_df.describe()
Out[66]:
	id 	created_at 	user_id 	user_name 	tweet_text 	url
count 	19 	19 	19 	19 	19 	19
unique 	7 	7 	6 	6 	6 	7
top 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://bit.ly/1Hfd0Xm
freq 	6 	6 	9 	9 	6 	6
In [69]:
#
# Blaze dataframe
#
twts_bz_df = Data(twts_csv_read, columns=Tweet01._fields)
In [70]:
twts_bz_df
Out[70]:
	id 	created_at 	user_id 	user_name 	tweet_text 	url
0 	id 	created_at 	user_id 	user_name 	tweet_text 	url
1 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
2 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
3 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://www.webex.com/ciscospark/
4 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://sparkjava.com/
5 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	https://www.sparkfun.com/
6 	598796205091500032 	2015-05-14 10:25:15 	48695135 	John Humphreys 	RT @PrabhaGana: What exactly is @ApacheSpark a... 	http://bit.ly/1JduSUT
7 	598788561127735296 	2015-05-14 09:54:52 	2385931712 	Leonardo D'Ambrosi 	RT @Ellen_Friedman: I'm still on Euro time. If... 	http://bit.ly/1Hfd0Xm
8 	598785545557438464 	2015-05-14 09:42:53 	461020977 	Alexey Kosenkov 	RT @Ellen_Friedman: I'm still on Euro time. If... 	http://bit.ly/1Hfd0Xm
9 	598782970082807808 	2015-05-14 09:32:39 	1377652806 	embeddedcomputer.nl 	RT @BigDataTechCon: Moving Rating Prediction w... 	http://buff.ly/1QBpk8J
10 	598777933730160640 	2015-05-14 09:12:38 	294862170 	Ellen Friedman 	I'm still on Euro time. If you are too check o... 	http://bit.ly/1Hfd0Xm
In [71]:
# remove first row as it is a duplicate with the header
twts_bz_df = twts_bz_df.drop(twts_bz_df.index[:1])
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
<ipython-input-71-4d6c97fc4371> in <module>()
      1 # remove first row as it is a duplicate with the header
----> 2 twts_bz_df = twts_bz_df.drop(twts_bz_df.index[:1])
/home/an/anaconda/lib/python2.7/site-packages/blaze/expr/expressions.pyc in __getattr__(self, key)
    163             pass
    164         try:
--> 165             result = object.__getattribute__(self, key)
    166         except AttributeError:
    167             fields = dict(zip(map(valid_identifier, self.fields),
AttributeError: 'InteractiveSymbol' object has no attribute 'drop'
In [72]:
#
# Blaze dataframe
#
twts_bz_df = Data(twts_pd_df)
In [73]:
twts_bz_df.schema
Out[73]:
dshape("""{
  id: ?string,
  created_at: ?string,
  user_id: ?string,
  user_name: ?string,
  tweet_text: ?string,
  url: ?string
  }""")
In [74]:
twts_bz_df.dshape
Out[74]:
dshape("""19 * {
  id: ?string,
  created_at: ?string,
  user_id: ?string,
  user_name: ?string,
  tweet_text: ?string,
  url: ?string
  }""")
In [75]:
twts_bz_df.data
Out[75]:
	id 	created_at 	user_id 	user_name 	tweet_text 	url
1 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
2 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
3 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://www.webex.com/ciscospark/
4 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://sparkjava.com/
5 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	https://www.sparkfun.com/
6 	598796205091500032 	2015-05-14 10:25:15 	48695135 	John Humphreys 	RT @PrabhaGana: What exactly is @ApacheSpark a... 	http://bit.ly/1JduSUT
7 	598788561127735296 	2015-05-14 09:54:52 	2385931712 	Leonardo D'Ambrosi 	RT @Ellen_Friedman: I'm still on Euro time. If... 	http://bit.ly/1Hfd0Xm
8 	598785545557438464 	2015-05-14 09:42:53 	461020977 	Alexey Kosenkov 	RT @Ellen_Friedman: I'm still on Euro time. If... 	http://bit.ly/1Hfd0Xm
9 	598782970082807808 	2015-05-14 09:32:39 	1377652806 	embeddedcomputer.nl 	RT @BigDataTechCon: Moving Rating Prediction w... 	http://buff.ly/1QBpk8J
10 	598777933730160640 	2015-05-14 09:12:38 	294862170 	Ellen Friedman 	I'm still on Euro time. If you are too check o... 	http://bit.ly/1Hfd0Xm
11 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
12 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://www.webex.com/ciscospark/
13 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://sparkjava.com/
14 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	https://www.sparkfun.com/
15 	598796205091500032 	2015-05-14 10:25:15 	48695135 	John Humphreys 	RT @PrabhaGana: What exactly is @ApacheSpark a... 	http://bit.ly/1JduSUT
16 	598788561127735296 	2015-05-14 09:54:52 	2385931712 	Leonardo D'Ambrosi 	RT @Ellen_Friedman: I'm still on Euro time. If... 	http://bit.ly/1Hfd0Xm
17 	598785545557438464 	2015-05-14 09:42:53 	461020977 	Alexey Kosenkov 	RT @Ellen_Friedman: I'm still on Euro time. If... 	http://bit.ly/1Hfd0Xm
18 	598782970082807808 	2015-05-14 09:32:39 	1377652806 	embeddedcomputer.nl 	RT @BigDataTechCon: Moving Rating Prediction w... 	http://buff.ly/1QBpk8J
19 	598777933730160640 	2015-05-14 09:12:38 	294862170 	Ellen Friedman 	I'm still on Euro time. If you are too check o... 	http://bit.ly/1Hfd0Xm
In [76]:
twts_bz_df.tweet_text.distinct()
Out[76]:
	tweet_text
0 	RT @pacoid: Great recap of @StrataConf EU in L...
1 	RT @alvaroagea: Simply @ApacheSpark http://t.c...
2 	RT @PrabhaGana: What exactly is @ApacheSpark a...
3 	RT @Ellen_Friedman: I'm still on Euro time. If...
4 	RT @BigDataTechCon: Moving Rating Prediction w...
5 	I'm still on Euro time. If you are too check o...
In [78]:
twts_bz_df[['id', 'user_name','tweet_text']].distinct()
Out[78]:
	id 	user_name 	tweet_text
0 	598831111406510082 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L...
1 	598808944719593472 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c...
2 	598796205091500032 	John Humphreys 	RT @PrabhaGana: What exactly is @ApacheSpark a...
3 	598788561127735296 	Leonardo D'Ambrosi 	RT @Ellen_Friedman: I'm still on Euro time. If...
4 	598785545557438464 	Alexey Kosenkov 	RT @Ellen_Friedman: I'm still on Euro time. If...
5 	598782970082807808 	embeddedcomputer.nl 	RT @BigDataTechCon: Moving Rating Prediction w...
6 	598777933730160640 	Ellen Friedman 	I'm still on Euro time. If you are too check o...
In [79]:
csvFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
csvFname = 'twtr15051401'
csvSuffix = 'csv'
os.path.isfile('{0}/{1}.{2}'.format(csvFpath, csvFname, csvSuffix))
Out[79]:
True
In [80]:
filepath   = csvFpath
filename   = csvFname
filesuffix = csvSuffix
twts_odo_df = Data('{0}/{1}.{2}'.format(filepath, filename, filesuffix))
In [81]:
twts_odo_df.count()
Out[81]:
19
In [82]:
twts_odo_df.head(5)
Out[82]:
	id 	created_at 	user_id 	user_name 	tweet_text 	url
0 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
1 	598831111406510082 	2015-05-14 12:43:57 	14755521 	raulsaeztapia 	RT @pacoid: Great recap of @StrataConf EU in L... 	http://www.mango-solutions.com/wp/2015/05/the-...
2 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://www.webex.com/ciscospark/
3 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	http://sparkjava.com/
4 	598808944719593472 	2015-05-14 11:15:52 	14755521 	raulsaeztapia 	RT @alvaroagea: Simply @ApacheSpark http://t.c... 	https://www.sparkfun.com/
In [83]:
twts_odo_df.dshape
Out[83]:
dshape("""var * {
  id: int64,
  created_at: ?datetime,
  user_id: int64,
  user_name: ?string,
  tweet_text: ?string,
  url: ?string
  }""")
In [84]:
twts_odo_df.id.distinct().count()
Out[84]:
7
In [85]:
twts_odo_df.id.distinct()
Out[85]:
	id
0 	598831111406510082
1 	598808944719593472
2 	598796205091500032
3 	598788561127735296
4 	598785545557438464
5 	598782970082807808
6 	598777933730160640
In [86]:
twts_odo_df[['user_id','user_name']].distinct()
Out[86]:
	user_id 	user_name
0 	14755521 	raulsaeztapia
1 	48695135 	John Humphreys
2 	2385931712 	Leonardo D'Ambrosi
3 	461020977 	Alexey Kosenkov
4 	1377652806 	embeddedcomputer.nl
5 	294862170 	Ellen Friedman
In [87]:
twts_odo_df.tweet_text.distinct()
Out[87]:
	tweet_text
0 	RT @pacoid: Great recap of @StrataConf EU in L...
1 	RT @alvaroagea: Simply @ApacheSpark http://t.c...
2 	RT @PrabhaGana: What exactly is @ApacheSpark a...
3 	RT @Ellen_Friedman: I'm still on Euro time. If...
4 	RT @BigDataTechCon: Moving Rating Prediction w...
5 	I'm still on Euro time. If you are too check o...
In [89]:
twts_odo_distinct_df = twts_odo_df[['id', 'user_name', 'user_id', 'tweet_text', 'created_at']].distinct()
In [91]:
jsonFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
jsonFname = 'twtr15051401_distinct' ## !! twtr15051401 reduced to only the distinct tweets
jsonSuffix = 'json'
In [92]:
odo(twts_odo_distinct_df, '{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix))
Out[92]:
<odo.backends.json.JSONLines at 0x7f77f0abfc50>
In [93]:
csvFpath  = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
csvFname  = 'twtr15052401_distinct' ## !! twtr15051401 reduced to only the distinct tweets
csvSuffix = 'csv'
In [94]:
odo('{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix), '{0}/{1}.{2}'.format(csvFpath, csvFname, csvSuffix))
Out[94]:
<odo.backends.csv.CSV at 0x7f77f0abfe10>
In [97]:
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
In [95]:
sc
Out[95]:
<pyspark.context.SparkContext at 0x7f7829581890>
In [96]:
sc.master
Out[96]:
u'local[*]'
In [98]:
# Instantiate Spark  SQL context
sqlc =  SQLContext(sc)
In [100]:
twts_sql_df_01 = sqlc.jsonFile("/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data/twtr15051401_distinct.json")
In [101]:
twts_sql_df_01.show()
created_at           id                 tweet_text           user_id    user_name          
2015-05-14T12:43:57Z 598831111406510082 RT @pacoid: Great... 14755521   raulsaeztapia      
2015-05-14T11:15:52Z 598808944719593472 RT @alvaroagea: S... 14755521   raulsaeztapia      
2015-05-14T10:25:15Z 598796205091500032 RT @PrabhaGana: W... 48695135   John Humphreys     
2015-05-14T09:54:52Z 598788561127735296 RT @Ellen_Friedma... 2385931712 Leonardo D'Ambrosi 
2015-05-14T09:42:53Z 598785545557438464 RT @Ellen_Friedma... 461020977  Alexey Kosenkov    
2015-05-14T09:32:39Z 598782970082807808 RT @BigDataTechCo... 1377652806 embeddedcomputer.nl
2015-05-14T09:12:38Z 598777933730160640 I'm still on Euro... 294862170  Ellen Friedman     
In [102]:
twts_sql_df_01.printSchema()
root
 |-- created_at: string (nullable = true)
 |-- id: long (nullable = true)
 |-- tweet_text: string (nullable = true)
 |-- user_id: long (nullable = true)
 |-- user_name: string (nullable = true)
In [105]:
twts_sql_df_01.select('user_name').show()
user_name          
raulsaeztapia      
raulsaeztapia      
John Humphreys     
Leonardo D'Ambrosi 
Alexey Kosenkov    
embeddedcomputer.nl
Ellen Friedman     
In [106]:
twts_sql_df_01.registerAsTable('tweets_01')
In [107]:
twts_sql_df_01_selection = sqlc.sql("SELECT * FROM tweets_01 WHERE user_name = 'raulsaeztapia'")
In [109]:
twts_sql_df_01_selection.show()
created_at           id                 tweet_text           user_id  user_name    
2015-05-14T12:43:57Z 598831111406510082 RT @pacoid: Great... 14755521 raulsaeztapia
2015-05-14T11:15:52Z 598808944719593472 RT @alvaroagea: S... 14755521 raulsaeztapia
In [110]:
jsonFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
jsonFname = 'twtr15051401'
jsonSuffix = 'json'
infile = ('{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix))
os.path.isfile('{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix))
Out[110]:
True
In [111]:
infile
Out[111]:
'/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data/twtr15051401.json'
In [112]:
# parse multi-line json file
twts_sc_json_01 = sc.textFile(infile).map(lambda x: json.loads(x))

In [123]:
tweets_sqlc_inf = sqlc.jsonFile(infile)
In [124]:
tweets_sqlc_inf.first()
Out[124]:
Row(contributors=None, coordinates=None, created_at=u'Thu May 14 12:43:57 +0000 2015', entities=Row(hashtags=[], media=None, symbols=[], urls=[Row(display_url=u'mango-solutions.com/wp/2015/05/the\u2026', expanded_url=u'http://www.mango-solutions.com/wp/2015/05/the-2015-strata-hadoop-world-london/', indices=[111, 133], url=u'http://t.co/aayCExDbqh')], user_mentions=[Row(id=14066472, id_str=u'14066472', indices=[3, 10], name=u'paco nathan', screen_name=u'pacoid'), Row(id=167169119, id_str=u'167169119', indices=[27, 38], name=u"O'Reilly Strata", screen_name=u'strataconf'), Row(id=1551361069, id_str=u'1551361069', indices=[58, 70], name=u'Apache Spark', screen_name=u'ApacheSpark')]), favorite_count=0, favorited=False, geo=None, id=598831111406510082, id_str=u'598831111406510082', in_reply_to_screen_name=None, in_reply_to_status_id=None, in_reply_to_status_id_str=None, in_reply_to_user_id=None, in_reply_to_user_id_str=None, lang=u'en', metadata=Row(iso_language_code=u'en', result_type=u'recent'), place=None, possibly_sensitive=False, retweet_count=7, retweeted=False, retweeted_status=Row(contributors=None, coordinates=Row(coordinates=[0.0, 0.0], type=u'Point'), created_at=u'Wed May 13 14:34:52 +0000 2015', entities=Row(hashtags=[], media=None, symbols=[], urls=[Row(display_url=u'mango-solutions.com/wp/2015/05/the\u2026', expanded_url=u'http://www.mango-solutions.com/wp/2015/05/the-2015-strata-hadoop-world-london/', indices=[99, 121], url=u'http://t.co/aayCExDbqh')], user_mentions=[Row(id=167169119, id_str=u'167169119', indices=[15, 26], name=u"O'Reilly Strata", screen_name=u'strataconf'), Row(id=1551361069, id_str=u'1551361069', indices=[46, 58], name=u'Apache Spark', screen_name=u'ApacheSpark')]), favorite_count=8, favorited=False, geo=Row(coordinates=[0.0, 0.0], type=u'Point'), id=598496635706970113, id_str=u'598496635706970113', in_reply_to_screen_name=None, in_reply_to_status_id=None, in_reply_to_status_id_str=None, in_reply_to_user_id=None, in_reply_to_user_id_str=None, lang=u'en', metadata=Row(iso_language_code=u'en', result_type=u'recent'), place=Row(bounding_box=Row(coordinates=[[[-122.117916, 37.3567709], [-122.044969, 37.3567709], [-122.044969, 37.436935], [-122.117916, 37.436935]]], type=u'Polygon'), contained_within=[], country=u'United States', country_code=u'US', full_name=u'Mountain View, CA', id=u'b19a2cc5134b7e0a', name=u'Mountain View', place_type=u'city', url=u'https://api.twitter.com/1.1/geo/id/b19a2cc5134b7e0a.json'), possibly_sensitive=False, retweet_count=7, retweeted=False, source=u'<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>', text=u'Great recap of @StrataConf EU in London, with @ApacheSpark summary: DataFrame, R integration, etc. http://t.co/aayCExDbqh', truncated=False, user=Row(contributors_enabled=False, created_at=u'Sat Mar 01 21:45:12 +0000 2008', default_profile=False, default_profile_image=False, description=u'Evil Mad Scientist; @OReillyMedia author; @AmplifyPartners adv.; treehugging SLO native #Green; formerly #FringeWare, etc.; deleter of DMs', entities=Row(description=Row(urls=[]), url=Row(urls=[Row(display_url=u'liber118.com/pxn/', expanded_url=u'http://liber118.com/pxn/', indices=[0, 22], url=u'http://t.co/rmgmMZ8LWd')])), favourites_count=5790, follow_request_sent=False, followers_count=5351, following=False, friends_count=5272, geo_enabled=True, id=14066472, id_str=u'14066472', is_translation_enabled=False, is_translator=False, lang=u'en', listed_count=301, location=u'Ecotopia', name=u'paco nathan', notifications=False, profile_background_color=u'A1DA40', profile_background_image_url=u'http://abs.twimg.com/images/themes/theme1/bg.png', profile_background_image_url_https=u'https://abs.twimg.com/images/themes/theme1/bg.png', profile_background_tile=False, profile_banner_url=u'https://pbs.twimg.com/profile_banners/14066472/1398212488', profile_image_url=u'http://pbs.twimg.com/profile_images/897599578/pinguino_normal.jpg', profile_image_url_https=u'https://pbs.twimg.com/profile_images/897599578/pinguino_normal.jpg', profile_link_color=u'CCD24E', profile_sidebar_border_color=u'CBD2C2', profile_sidebar_fill_color=u'82857B', profile_text_color=u'FDC600', profile_use_background_image=False, protected=False, screen_name=u'pacoid', statuses_count=7914, time_zone=u'Pacific Time (US & Canada)', url=u'http://t.co/rmgmMZ8LWd', utc_offset=-25200, verified=False)), source=u'<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>', text=u'RT @pacoid: Great recap of @StrataConf EU in London, with @ApacheSpark summary: DataFrame, R integration, etc. http://t.co/aayCExDbqh', truncated=False, user=Row(contributors_enabled=False, created_at=u'Tue May 13 06:19:52 +0000 2008', default_profile=True, default_profile_image=False, description=u'', entities=Row(description=Row(urls=[]), url=None), favourites_count=190, follow_request_sent=False, followers_count=73, following=False, friends_count=121, geo_enabled=False, id=14755521, id_str=u'14755521', is_translation_enabled=False, is_translator=False, lang=u'es', listed_count=21, location=u'', name=u'raulsaeztapia', notifications=False, profile_background_color=u'C0DEED', profile_background_image_url=u'http://abs.twimg.com/images/themes/theme1/bg.png', profile_background_image_url_https=u'https://abs.twimg.com/images/themes/theme1/bg.png', profile_background_tile=False, profile_banner_url=None, profile_image_url=u'http://pbs.twimg.com/profile_images/521604886/dsc00229_normal.jpg', profile_image_url_https=u'https://pbs.twimg.com/profile_images/521604886/dsc00229_normal.jpg', profile_link_color=u'0084B4', profile_sidebar_border_color=u'C0DEED', profile_sidebar_fill_color=u'DDEEF6', profile_text_color=u'333333', profile_use_background_image=True, protected=False, screen_name=u'raulsaeztapia', statuses_count=462, time_zone=u'Madrid', url=None, utc_offset=7200, verified=False))
In [125]:
tweets_sqlc_inf.printSchema()
root
 |-- contributors: string (nullable = true)
 |-- coordinates: string (nullable = true)
 |-- created_at: string (nullable = true)
 |-- entities: struct (nullable = true)
 |    |-- hashtags: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |-- text: string (nullable = true)
 |    |-- media: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- display_url: string (nullable = true)
 |    |    |    |-- expanded_url: string (nullable = true)
 |    |    |    |-- id: long (nullable = true)
 |    |    |    |-- id_str: string (nullable = true)
 |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |-- media_url: string (nullable = true)
 |    |    |    |-- media_url_https: string (nullable = true)
 |    |    |    |-- sizes: struct (nullable = true)
 |    |    |    |    |-- large: struct (nullable = true)
 |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |-- medium: struct (nullable = true)
 |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |-- small: struct (nullable = true)
 |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |-- thumb: struct (nullable = true)
 |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |-- source_status_id: long (nullable = true)
 |    |    |    |-- source_status_id_str: string (nullable = true)
 |    |    |    |-- source_user_id: long (nullable = true)
 |    |    |    |-- source_user_id_str: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |    |-- url: string (nullable = true)
 |    |-- symbols: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- urls: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- display_url: string (nullable = true)
 |    |    |    |-- expanded_url: string (nullable = true)
 |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |-- url: string (nullable = true)
 |    |-- user_mentions: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: long (nullable = true)
 |    |    |    |-- id_str: string (nullable = true)
 |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- screen_name: string (nullable = true)
 |-- favorite_count: long (nullable = true)
 |-- favorited: boolean (nullable = true)
 |-- geo: string (nullable = true)
 |-- id: long (nullable = true)
 |-- id_str: string (nullable = true)
 |-- in_reply_to_screen_name: string (nullable = true)
 |-- in_reply_to_status_id: string (nullable = true)
 |-- in_reply_to_status_id_str: string (nullable = true)
 |-- in_reply_to_user_id: long (nullable = true)
 |-- in_reply_to_user_id_str: string (nullable = true)
 |-- lang: string (nullable = true)
 |-- metadata: struct (nullable = true)
 |    |-- iso_language_code: string (nullable = true)
 |    |-- result_type: string (nullable = true)
 |-- place: string (nullable = true)
 |-- possibly_sensitive: boolean (nullable = true)
 |-- retweet_count: long (nullable = true)
 |-- retweeted: boolean (nullable = true)
 |-- retweeted_status: struct (nullable = true)
 |    |-- contributors: string (nullable = true)
 |    |-- coordinates: struct (nullable = true)
 |    |    |-- coordinates: array (nullable = true)
 |    |    |    |-- element: double (containsNull = true)
 |    |    |-- type: string (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- entities: struct (nullable = true)
 |    |    |-- hashtags: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |-- text: string (nullable = true)
 |    |    |-- media: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- display_url: string (nullable = true)
 |    |    |    |    |-- expanded_url: string (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- id_str: string (nullable = true)
 |    |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |-- media_url: string (nullable = true)
 |    |    |    |    |-- media_url_https: string (nullable = true)
 |    |    |    |    |-- sizes: struct (nullable = true)
 |    |    |    |    |    |-- large: struct (nullable = true)
 |    |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |    |-- medium: struct (nullable = true)
 |    |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |    |-- small: struct (nullable = true)
 |    |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |    |-- thumb: struct (nullable = true)
 |    |    |    |    |    |    |-- h: long (nullable = true)
 |    |    |    |    |    |    |-- resize: string (nullable = true)
 |    |    |    |    |    |    |-- w: long (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |    |-- url: string (nullable = true)
 |    |    |-- symbols: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- urls: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- display_url: string (nullable = true)
 |    |    |    |    |-- expanded_url: string (nullable = true)
 |    |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |-- url: string (nullable = true)
 |    |    |-- user_mentions: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- id_str: string (nullable = true)
 |    |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- screen_name: string (nullable = true)
 |    |-- favorite_count: long (nullable = true)
 |    |-- favorited: boolean (nullable = true)
 |    |-- geo: struct (nullable = true)
 |    |    |-- coordinates: array (nullable = true)
 |    |    |    |-- element: double (containsNull = true)
 |    |    |-- type: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- id_str: string (nullable = true)
 |    |-- in_reply_to_screen_name: string (nullable = true)
 |    |-- in_reply_to_status_id: string (nullable = true)
 |    |-- in_reply_to_status_id_str: string (nullable = true)
 |    |-- in_reply_to_user_id: long (nullable = true)
 |    |-- in_reply_to_user_id_str: string (nullable = true)
 |    |-- lang: string (nullable = true)
 |    |-- metadata: struct (nullable = true)
 |    |    |-- iso_language_code: string (nullable = true)
 |    |    |-- result_type: string (nullable = true)
 |    |-- place: struct (nullable = true)
 |    |    |-- bounding_box: struct (nullable = true)
 |    |    |    |-- coordinates: array (nullable = true)
 |    |    |    |    |-- element: array (containsNull = true)
 |    |    |    |    |    |-- element: array (containsNull = true)
 |    |    |    |    |    |    |-- element: double (containsNull = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |-- contained_within: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- country_code: string (nullable = true)
 |    |    |-- full_name: string (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- place_type: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |-- possibly_sensitive: boolean (nullable = true)
 |    |-- retweet_count: long (nullable = true)
 |    |-- retweeted: boolean (nullable = true)
 |    |-- source: string (nullable = true)
 |    |-- text: string (nullable = true)
 |    |-- truncated: boolean (nullable = true)
 |    |-- user: struct (nullable = true)
 |    |    |-- contributors_enabled: boolean (nullable = true)
 |    |    |-- created_at: string (nullable = true)
 |    |    |-- default_profile: boolean (nullable = true)
 |    |    |-- default_profile_image: boolean (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- entities: struct (nullable = true)
 |    |    |    |-- description: struct (nullable = true)
 |    |    |    |    |-- urls: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- url: struct (nullable = true)
 |    |    |    |    |-- urls: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- display_url: string (nullable = true)
 |    |    |    |    |    |    |-- expanded_url: string (nullable = true)
 |    |    |    |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |-- favourites_count: long (nullable = true)
 |    |    |-- follow_request_sent: boolean (nullable = true)
 |    |    |-- followers_count: long (nullable = true)
 |    |    |-- following: boolean (nullable = true)
 |    |    |-- friends_count: long (nullable = true)
 |    |    |-- geo_enabled: boolean (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- id_str: string (nullable = true)
 |    |    |-- is_translation_enabled: boolean (nullable = true)
 |    |    |-- is_translator: boolean (nullable = true)
 |    |    |-- lang: string (nullable = true)
 |    |    |-- listed_count: long (nullable = true)
 |    |    |-- location: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- notifications: boolean (nullable = true)
 |    |    |-- profile_background_color: string (nullable = true)
 |    |    |-- profile_background_image_url: string (nullable = true)
 |    |    |-- profile_background_image_url_https: string (nullable = true)
 |    |    |-- profile_background_tile: boolean (nullable = true)
 |    |    |-- profile_banner_url: string (nullable = true)
 |    |    |-- profile_image_url: string (nullable = true)
 |    |    |-- profile_image_url_https: string (nullable = true)
 |    |    |-- profile_link_color: string (nullable = true)
 |    |    |-- profile_sidebar_border_color: string (nullable = true)
 |    |    |-- profile_sidebar_fill_color: string (nullable = true)
 |    |    |-- profile_text_color: string (nullable = true)
 |    |    |-- profile_use_background_image: boolean (nullable = true)
 |    |    |-- protected: boolean (nullable = true)
 |    |    |-- screen_name: string (nullable = true)
 |    |    |-- statuses_count: long (nullable = true)
 |    |    |-- time_zone: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- utc_offset: long (nullable = true)
 |    |    |-- verified: boolean (nullable = true)
 |-- source: string (nullable = true)
 |-- text: string (nullable = true)
 |-- truncated: boolean (nullable = true)
 |-- user: struct (nullable = true)
 |    |-- contributors_enabled: boolean (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- default_profile: boolean (nullable = true)
 |    |-- default_profile_image: boolean (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- entities: struct (nullable = true)
 |    |    |-- description: struct (nullable = true)
 |    |    |    |-- urls: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |-- url: struct (nullable = true)
 |    |    |    |-- urls: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- display_url: string (nullable = true)
 |    |    |    |    |    |-- expanded_url: string (nullable = true)
 |    |    |    |    |    |-- indices: array (nullable = true)
 |    |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |    |-- url: string (nullable = true)
 |    |-- favourites_count: long (nullable = true)
 |    |-- follow_request_sent: boolean (nullable = true)
 |    |-- followers_count: long (nullable = true)
 |    |-- following: boolean (nullable = true)
 |    |-- friends_count: long (nullable = true)
 |    |-- geo_enabled: boolean (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- id_str: string (nullable = true)
 |    |-- is_translation_enabled: boolean (nullable = true)
 |    |-- is_translator: boolean (nullable = true)
 |    |-- lang: string (nullable = true)
 |    |-- listed_count: long (nullable = true)
 |    |-- location: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- notifications: boolean (nullable = true)
 |    |-- profile_background_color: string (nullable = true)
 |    |-- profile_background_image_url: string (nullable = true)
 |    |-- profile_background_image_url_https: string (nullable = true)
 |    |-- profile_background_tile: boolean (nullable = true)
 |    |-- profile_banner_url: string (nullable = true)
 |    |-- profile_image_url: string (nullable = true)
 |    |-- profile_image_url_https: string (nullable = true)
 |    |-- profile_link_color: string (nullable = true)
 |    |-- profile_sidebar_border_color: string (nullable = true)
 |    |-- profile_sidebar_fill_color: string (nullable = true)
 |    |-- profile_text_color: string (nullable = true)
 |    |-- profile_use_background_image: boolean (nullable = true)
 |    |-- protected: boolean (nullable = true)
 |    |-- screen_name: string (nullable = true)
 |    |-- statuses_count: long (nullable = true)
 |    |-- time_zone: string (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- utc_offset: long (nullable = true)
 |    |-- verified: boolean (nullable = true)
In [126]:
tweets_sqlc_inf.count()
Out[126]:
10L
In [127]:
tweets_sqlc_inf.explain()
PhysicalRDD [contributors#5,coordinates#6,created_at#7,entities#8,favorite_count#9L,favorited#10,geo#11,id#12L,id_str#13,in_reply_to_screen_name#14,in_reply_to_status_id#15,in_reply_to_status_id_str#16,in_reply_to_user_id#17L,in_reply_to_user_id_str#18,lang#19,metadata#20,place#21,possibly_sensitive#22,retweet_count#23L,retweeted#24,retweeted_status#25,source#26,text#27,truncated#28,user#29], MapPartitionsRDD[55] at map at JsonRDD.scala:41
In [128]:
type(tweets_sqlc_inf)
Out[128]:
pyspark.sql.dataframe.DataFrame
In [130]:
tweets_sqlc_inf.columns
Out[130]:
[u'contributors',
 u'coordinates',
 u'created_at',
 u'entities',
 u'favorite_count',
 u'favorited',
 u'geo',
 u'id',
 u'id_str',
 u'in_reply_to_screen_name',
 u'in_reply_to_status_id',
 u'in_reply_to_status_id_str',
 u'in_reply_to_user_id',
 u'in_reply_to_user_id_str',
 u'lang',
 u'metadata',
 u'place',
 u'possibly_sensitive',
 u'retweet_count',
 u'retweeted',
 u'retweeted_status',
 u'source',
 u'text',
 u'truncated',
 u'user']
In [144]:
tweets_extract_sqlc = tweets_sqlc_inf[['created_at', 'id', 'text', 'user.id', 'user.name', 'entities.urls.expanded_url']].distinct()
In [145]:
tweets_extract_sqlc.show()
created_at           id                 text                 id         name                expanded_url        
Thu May 14 09:32:... 598782970082807808 RT @BigDataTechCo... 1377652806 embeddedcomputer.nl ArrayBuffer(http:...
Thu May 14 12:43:... 598831111406510082 RT @pacoid: Great... 14755521   raulsaeztapia       ArrayBuffer(http:...
Thu May 14 12:18:... 598824733086523393 @rabbitonweb spea... 2787206368 Bartek Andrzejczak  ArrayBuffer()       
Thu May 14 09:42:... 598785545557438464 RT @Ellen_Friedma... 461020977  Alexey Kosenkov     ArrayBuffer(http:...
Thu May 14 09:12:... 598777933730160640 I'm still on Euro... 294862170  Ellen Friedman      ArrayBuffer(http:...
Thu May 14 11:15:... 598808944719593472 RT @alvaroagea: S... 14755521   raulsaeztapia       ArrayBuffer(http:...
Thu May 14 12:42:... 598830778269769728 @spark_io is now ... 15981533   Burr Sutter         ArrayBuffer()       
Thu May 14 09:54:... 598788561127735296 RT @Ellen_Friedma... 2385931712 Leonardo D'Ambrosi  ArrayBuffer(http:...
Thu May 14 10:25:... 598796205091500032 RT @PrabhaGana: W... 48695135   John Humphreys      ArrayBuffer(http:...
Thu May 14 12:28:... 598827171168264192 RT @baandrzejczak... 20909005   Paweł Szulc         ArrayBuffer()       
In [147]:
tweets_extract_sqlc.schema
Out[147]:
StructType(List(StructField(created_at,StringType,true),StructField(id,LongType,true),StructField(text,StringType,true),StructField(id,LongType,true),StructField(name,StringType,true),StructField(expanded_url,ArrayType(StringType,true),true)))
In [168]:
tweets_extract_sqlc.registerTempTable("Tweets_xtr_001")
In [169]:
%sql describe Tweets_xtr_001
ERROR: Line magic function `%sql` not found.
In [171]:
sqlc.sql("DESCRIBE Tweets_xtr_001")
Out[171]:
DataFrame[col_name: string, data_type: string, comment: string]

In [174]:
tweets_extract_sqlc_sel = sqlc.sql("SELECT * from Tweets_xtr_001 WHERE name='raulsaeztapia'")
In [176]:
tweets_extract_sqlc_sel.explain(extended = True)
== Parsed Logical Plan ==
'Project [*]
 'Filter ('name = raulsaeztapia)
  'UnresolvedRelation [Tweets_xtr_001], None
== Analyzed Logical Plan ==
Project [created_at#7,id#12L,text#27,id#80L,name#81,expanded_url#82]
 Filter (name#81 = raulsaeztapia)
  Distinct 
   Project [created_at#7,id#12L,text#27,user#29.id AS id#80L,user#29.name AS name#81,entities#8.urls.expanded_url AS expanded_url#82]
    Relation[contributors#5,coordinates#6,created_at#7,entities#8,favorite_count#9L,favorited#10,geo#11,id#12L,id_str#13,in_reply_to_screen_name#14,in_reply_to_status_id#15,in_reply_to_status_id_str#16,in_reply_to_user_id#17L,in_reply_to_user_id_str#18,lang#19,metadata#20,place#21,possibly_sensitive#22,retweet_count#23L,retweeted#24,retweeted_status#25,source#26,text#27,truncated#28,user#29] JSONRelation(/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data/twtr15051401.json,1.0,None)
== Optimized Logical Plan ==
Filter (name#81 = raulsaeztapia)
 Distinct 
  Project [created_at#7,id#12L,text#27,user#29.id AS id#80L,user#29.name AS name#81,entities#8.urls.expanded_url AS expanded_url#82]
   Relation[contributors#5,coordinates#6,created_at#7,entities#8,favorite_count#9L,favorited#10,geo#11,id#12L,id_str#13,in_reply_to_screen_name#14,in_reply_to_status_id#15,in_reply_to_status_id_str#16,in_reply_to_user_id#17L,in_reply_to_user_id_str#18,lang#19,metadata#20,place#21,possibly_sensitive#22,retweet_count#23L,retweeted#24,retweeted_status#25,source#26,text#27,truncated#28,user#29] JSONRelation(/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data/twtr15051401.json,1.0,None)
== Physical Plan ==
Filter (name#81 = raulsaeztapia)
 Distinct false
  Exchange (HashPartitioning [created_at#7,id#12L,text#27,id#80L,name#81,expanded_url#82], 200)
   Distinct true
    Project [created_at#7,id#12L,text#27,user#29.id AS id#80L,user#29.name AS name#81,entities#8.urls.expanded_url AS expanded_url#82]
     PhysicalRDD [contributors#5,coordinates#6,created_at#7,entities#8,favorite_count#9L,favorited#10,geo#11,id#12L,id_str#13,in_reply_to_screen_name#14,in_reply_to_status_id#15,in_reply_to_status_id_str#16,in_reply_to_user_id#17L,in_reply_to_user_id_str#18,lang#19,metadata#20,place#21,possibly_sensitive#22,retweet_count#23L,retweeted#24,retweeted_status#25,source#26,text#27,truncated#28,user#29], MapPartitionsRDD[165] at map at JsonRDD.scala:41
Code Generation: false
== RDD ==
In [175]:
tweets_extract_sqlc_sel.show()
created_at           id                 text                 id       name          expanded_url        
Thu May 14 12:43:... 598831111406510082 RT @pacoid: Great... 14755521 raulsaeztapia ArrayBuffer(http:...
Thu May 14 11:15:... 598808944719593472 RT @alvaroagea: S... 14755521 raulsaeztapia ArrayBuffer(http:...
In [148]:
tweets_extract_sqlc.explain
Out[148]:
<bound method DataFrame.explain of DataFrame[created_at: string, id: bigint, text: string, id: bigint, name: string, expanded_url: array<string>]>
In [152]:
tweets_extracted_sqlc_json = tweets_extract_sqlc.toJSON()
In [153]:
type(tweets_extracted_sqlc_json)
Out[153]:
pyspark.rdd.RDD
In [158]:
jsonFpath = '/home/an/spark/spark-1.3.0-bin-hadoop2.4/examples/AN_Spark/data'
jsonFname = 'twtr15053001_xtrct_sqlc'
jsonSuffix = 'json'
outfile = ('{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix))
os.path.isfile('{0}/{1}.{2}'.format(jsonFpath, jsonFname, jsonSuffix))
Out[158]:
False
In [159]:
tweets_extracted_sqlc_json.saveAsTextFile(outfile)
