#
#
#
#

Jupyter Notebook
A22_Spark_Visualization_Bokeh_Worldcloud Last Checkpoint: 3 hours ago (autosaved)
Python 2

In [ ]:
#
# AN - 2015-11-08
# Python Bokeh _ Spark _ Visualizations
# 1- Data Preparation
# 2- Wordcloud
# 3- Geo-Location Tweets 
# 4- Google Map for Meetups in London
#
In [ ]:
#
# Launch command
# an@an-VB:~/spark/spark-1.5.0-bin-hadoop2.6/examples/AN_Spark$ 
# IPYTHON_OPTS='notebook' /home/an/spark/spark-1.5.0-bin-hadoop2.6/bin/pyspark --packages com.databricks:spark-csv_2.11:1.2.0


​
In [16]:
#
# Read csv in a Panda DF
#
#
import pandas as pd
csv_in = '/home/an/spark/spark-1.5.0-bin-hadoop2.6/examples/AN_Spark/data/unq_tweetstxt.csv'
pddf_in = pd.read_csv(csv_in, index_col=None, header=0, sep=';', encoding='utf-8')
In [20]:
print('tweets pandas dataframe - count:', pddf_in.count())
print('tweets pandas dataframe - shape:', pddf_in.shape)
print('tweets pandas dataframe - colns:', pddf_in.columns)
('tweets pandas dataframe - count:', Unnamed: 0    7540
id            7540
created_at    7540
user_id       7540
user_name     7538
tweet_text    7540
dtype: int64)
('tweets pandas dataframe - shape:', (7540, 6))
('tweets pandas dataframe - colns:', Index([u'Unnamed: 0', u'id', u'created_at', u'user_id', u'user_name', u'tweet_text'], dtype='object'))
In [21]:
pddf_in.head()
Out[21]:
	Unnamed: 0 	id 	created_at 	user_id 	user_name 	tweet_text
0 	0 	638830426971181057 	Tue Sep 01 21:46:57 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: 9_A_6: dreamint...
1 	1 	638830426727911424 	Tue Sep 01 21:46:57 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews...
2 	2 	638830425402556417 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: 9_A_6: ernestsg...
3 	3 	638830424563716097 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews...
4 	4 	638830422256816132 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: elsahel12: 9_A_6: dreamintention...

In [72]:
import re
import time
In [24]:
regexp = {"RT": "^RT", "MT": r"^MT", "ALNUM": r"(@[a-zA-Z0-9_]+)",
          "HASHTAG": r"(#[\w\d]+)", "URL": r"([https://|http://]?[a-zA-Z\d\/]+[\.]+[a-zA-Z\d\/\.]+)",
          "SPACES":r"\s+"}
regexp = dict((key, re.compile(value)) for key, value in regexp.items())
In [25]:
regexp
Out[25]:
{'ALNUM': re.compile(r'(@[a-zA-Z0-9_]+)'),
 'HASHTAG': re.compile(r'(#[\w\d]+)'),
 'MT': re.compile(r'^MT'),
 'RT': re.compile(r'^RT'),
 'SPACES': re.compile(r'\s+'),
 'URL': re.compile(r'([https://|http://]?[a-zA-Z\d\/]+[\.]+[a-zA-Z\d\/\.]+)')}
In [77]:
def getAttributeRT(tweet):
    """ see if tweet is a RT """
    return re.search(regexp["RT"], tweet.strip()) != None
​
def getAttributeMT(tweet):
    """ see if tweet is a MT """
    return re.search(regexp["MT"], tweet.strip()) != None
​
def getUserHandles(tweet):
    """ given a tweet we try and extract all user handles in order of occurrence"""
    return re.findall(regexp["ALNUM"], tweet)
​
def getHashtags(tweet):
    """ return all hashtags"""
    return re.findall(regexp["HASHTAG"], tweet)
​
def getURLs(tweet):
    """ URL : [http://]?[\w\.?/]+"""
    return re.findall(regexp["URL"], tweet)
​
def getTextNoURLsUsers(tweet):
    """ return parsed text terms stripped of URLS and User Names in tweet text
        ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",x).split()) """
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(RT)"," ", tweet).lower().split())
​
def setTag(tweet):
    """ set tags to tweet_text based on search terms from tags_list"""
    tags_list = ['spark', 'python', 'clinton', 'trump', 'gaga', 'bieber']
    lower_text = tweet.lower()
    return filter(lambda x:x.lower() in lower_text,tags_list)
​
def decode_date(s):
    """ parse Twitter date into format yyyy-mm-dd hh:mm:ss"""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(s,'%a %b %d %H:%M:%S +0000 %Y'))

In [43]:
pddf_in.columns
Out[43]:
Index([u'Unnamed: 0', u'id', u'created_at', u'user_id', u'user_name', u'tweet_text'], dtype='object')
In [45]:
# df.drop([Column Name or list],inplace=True,axis=1)
pddf_in.drop(['Unnamed: 0'], inplace=True, axis=1)
In [46]:
pddf_in.head()
Out[46]:
	id 	created_at 	user_id 	user_name 	tweet_text
0 	638830426971181057 	Tue Sep 01 21:46:57 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: 9_A_6: dreamint...
1 	638830426727911424 	Tue Sep 01 21:46:57 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews...
2 	638830425402556417 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: 9_A_6: ernestsg...
3 	638830424563716097 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews...
4 	638830422256816132 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: elsahel12: 9_A_6: dreamintention...
In [82]:
pddf_in['htag'] = pddf_in.tweet_text.apply(getHashtags)
pddf_in['user_handles'] = pddf_in.tweet_text.apply(getUserHandles)
pddf_in['urls'] = pddf_in.tweet_text.apply(getURLs)
pddf_in['txt_terms'] = pddf_in.tweet_text.apply(getTextNoURLsUsers)
pddf_in['search_grp'] = pddf_in.tweet_text.apply(setTag)
pddf_in['date'] = pddf_in.created_at.apply(decode_date)
In [83]:
pddf_in[2200:2210]
Out[83]:
	id 	created_at 	user_id 	user_name 	tweet_text 	htag 	urls 	ptxt 	tgrp 	date 	user_handles 	txt_terms 	search_grp
2200 	638242693374681088 	Mon Aug 31 06:51:30 +0000 2015 	19525954 	CENATIC 	El impacto de @ApacheSpark en el procesamiento... 	[#sparkSpecial] 	[://t.co/4PQmJNuEJB] 	el impacto de en el procesamiento de datos y e... 	[spark] 	2015-08-31 06:51:30 	[@ApacheSpark] 	el impacto de en el procesamiento de datos y e... 	[spark]
2201 	638238014695575552 	Mon Aug 31 06:32:55 +0000 2015 	51115854 	Nawfal 	Real Time Streaming with Apache Spark\nhttp://... 	[#IoT, #SmartMelboune, #BigData, #Apachespark] 	[://t.co/GW5PaqwVab] 	real time streaming with apache spark iot smar... 	[spark] 	2015-08-31 06:32:55 	[] 	real time streaming with apache spark iot smar... 	[spark]
2202 	638236084124516352 	Mon Aug 31 06:25:14 +0000 2015 	62885987 	Mithun Katti 	RT @differentsachin: Spark the flame of digita... 	[#IBMHackathon, #SparkHackathon, #ISLconnectIN... 	[] 	spark the flame of digital india ibmhackathon ... 	[spark] 	2015-08-31 06:25:14 	[@differentsachin, @ApacheSpark] 	spark the flame of digital india ibmhackathon ... 	[spark]
2203 	638234734649176064 	Mon Aug 31 06:19:53 +0000 2015 	140462395 	solaimurugan v 	Installing @ApacheMahout with @ApacheSpark 1.4... 	[] 	[1.4.1, ://t.co/3c5dGbfaZe.] 	installing with 1 4 1 got many more issue whil... 	[spark] 	2015-08-31 06:19:53 	[@ApacheMahout, @ApacheSpark] 	installing with 1 4 1 got many more issue whil... 	[spark]
2204 	638233517307072512 	Mon Aug 31 06:15:02 +0000 2015 	2428473836 	Ralf Heineke 	RT @RomeoKienzler: Join me @velocityconf on #m... 	[#machinelearning, #devOps, #Bl] 	[://t.co/U5xL7pYEmF] 	join me on machinelearning based devops operat... 	[spark] 	2015-08-31 06:15:02 	[@RomeoKienzler, @velocityconf, @ApacheSpark] 	join me on machinelearning based devops operat... 	[spark]
2205 	638230184848687106 	Mon Aug 31 06:01:48 +0000 2015 	289355748 	Akim Boyko 	RT @databricks: Watch live today at 10am PT is... 	[] 	[1.5, ://t.co/16cix6ASti] 	watch live today at 10am pt is 1 5 presented b... 	[spark] 	2015-08-31 06:01:48 	[@databricks, @ApacheSpark, @databricks, @pwen... 	watch live today at 10am pt is 1 5 presented b... 	[spark]
2206 	638227830443110400 	Mon Aug 31 05:52:27 +0000 2015 	145001241 	sachin aggarwal 	Spark the flame of digital India @ #IBMHackath... 	[#IBMHackathon, #SparkHackathon, #ISLconnectIN... 	[://t.co/C1AO3uNexe] 	spark the flame of digital india ibmhackathon ... 	[spark] 	2015-08-31 05:52:27 	[@ApacheSpark] 	spark the flame of digital india ibmhackathon ... 	[spark]
2207 	638227031268810752 	Mon Aug 31 05:49:16 +0000 2015 	145001241 	sachin aggarwal 	RT @pravin_gadakh: Imagine, innovate and Igni... 	[#IBMHackathon, #ISLconnectIN2015] 	[] 	gadakh imagine innovate and ignite digital ind... 	[spark] 	2015-08-31 05:49:16 	[@pravin_gadakh, @ApacheSpark] 	gadakh imagine innovate and ignite digital ind... 	[spark]
2208 	638224591920336896 	Mon Aug 31 05:39:35 +0000 2015 	494725634 	IBM Asia Pacific 	RT @sachinparmar: Passionate about Spark?? Hav... 	[#IBMHackathon, #ISLconnectIN] 	[India..] 	passionate about spark have dreams of clean sa... 	[spark] 	2015-08-31 05:39:35 	[@sachinparmar] 	passionate about spark have dreams of clean sa... 	[spark]
2209 	638223327467692032 	Mon Aug 31 05:34:33 +0000 2015 	3158070968 	Open Source India 	"Game Changer" #ApacheSpark speeds up #bigdata... 	[#ApacheSpark, #bigdata] 	[://t.co/ieTQ9ocMim] 	game changer apachespark speeds up bigdata pro... 	[spark] 	2015-08-31 05:34:33 	[] 	game changer apachespark speeds up bigdata pro... 	[spark]
In [84]:
f_name = '/home/an/spark/spark-1.5.0-bin-hadoop2.6/examples/AN_Spark/data/unq_tweets_processed.csv'
pddf_in.to_csv(f_name, sep=';', encoding='utf-8', index=False)
In [85]:
pddf_in.shape
Out[85]:
(7540, 13)
#
#
#

In [21]:
import pandas as pd
csv_in = '/home/an/spark/spark-1.5.0-bin-hadoop2.6/examples/AN_Spark/data/spark_tweets.csv'
tspark_df = pd.read_csv(csv_in, index_col=None, header=0, sep=',', encoding='utf-8')
In [3]:
tspark_df.head(3)
Out[3]:
	id 	created_at 	user_id 	user_name 	tweet_text 	htag 	urls 	ptxt 	tgrp 	date 	user_handles 	txt_terms 	search_grp
0 	638818911773856000 	Tue Sep 01 21:01:11 +0000 2015 	2511247075 	Noor Din 	RT @kdnuggets: R leads RapidMiner, Python catc... 	[#KDN] 	[://t.co/3bsaTT7eUs] 	r leads rapidminer python catches up big data ... 	[spark, python] 	2015-09-01 21:01:11 	[@kdnuggets] 	r leads rapidminer python catches up big data ... 	[spark, python]
1 	622142176768737000 	Fri Jul 17 20:33:48 +0000 2015 	24537879 	IBM Cloudant 	Be one of the first to sign-up for IBM Analyti... 	[#ApacheSpark, #SparkInsight] 	[://t.co/C5TZpetVA6, ://t.co/R1L29DePaQ] 	be one of the first to sign up for ibm analyti... 	[spark] 	2015-07-17 20:33:48 	[] 	be one of the first to sign up for ibm analyti... 	[spark]
2 	622140453069169000 	Fri Jul 17 20:26:57 +0000 2015 	515145898 	Arno Candel 	Nice article on #apachespark, #hadoop and #dat... 	[#apachespark, #hadoop, #datascience] 	[://t.co/IyF44pV0f3] 	nice article on apachespark hadoop and datasci... 	[spark] 	2015-07-17 20:26:57 	[@h2oai] 	nice article on apachespark hadoop and datasci... 	[spark]
In [4]:
%matplotlib inline
In [11]:
len(tspark_df['txt_terms'].tolist())
Out[11]:
2024
In [22]:
tspark_ls_str = [str(t) for t in tspark_df['txt_terms'].tolist()]
In [14]:
len(tspark_ls_str)
Out[14]:
2024
In [15]:
tspark_ls_str[:4]
Out[15]:
['r leads rapidminer python catches up big data tools grow spark ignites kdn',
 'be one of the first to sign up for ibm analytics for apachespark today sparkinsight',
 'nice article on apachespark hadoop and datascience',
 'spark 101 running spark and mapreduce together in production hadoopsummit2015 apachespark altiscale']
In [23]:
​
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
​
# join tweets to a single string
words = ' '.join(tspark_ls_str)
​
# create wordcloud 
wordcloud = WordCloud(
                      # remove stopwords
                      stopwords=STOPWORDS,
                      background_color='black',
                      width=1800,
                      height=1400
                     ).generate(words)
​
# render wordcloud image
plt.imshow(wordcloud)
plt.axis('off')

# save wordcloud image on disk
plt.savefig('./spark_tweets_wordcloud_1.png', dpi=300)

# display image in Jupyter notebook
plt.show()

#
# Tweets Geo-Location on World Map
#

In [4]:
'''
This module exposes geometry data for World Country Boundaries.
​
'''
import csv
import codecs
import gzip
import xml.etree.cElementTree as et
import os
from os.path import dirname, join
​
nan = float('NaN')
__file__ = os.getcwd()
​
data = {}
with gzip.open(join(dirname(__file__), 'AN_Spark/data/World_Country_Boundaries.csv.gz')) as f:
    decoded = codecs.iterdecode(f, "utf-8")
    next(decoded)
    reader = csv.reader(decoded, delimiter=',', quotechar='"')
    for row in reader:
        geometry, code, name = row
        xml = et.fromstring(geometry)
        lats = []
        lons = []
        for i, poly in enumerate(xml.findall('.//outerBoundaryIs/LinearRing/coordinates')):
            if i > 0:
                lats.append(nan)
                lons.append(nan)
            coords = (c.split(',')[:2] for c in poly.text.split())
            lat, lon = list(zip(*[(float(lat), float(lon)) for lon, lat in
                coords]))
            lats.extend(lat)
            lons.extend(lon)
        data[code] = {
            'name'   : name,
            'lats'   : lats,
            'lons'   : lons,
        }
In [5]:
len(data)
Out[5]:
235
In [69]:
# data
#
#
In [8]:
import pandas as pd
csv_in = '/home/an/spark/spark-1.5.0-bin-hadoop2.6/examples/AN_Spark/data/spark_tweets_20.csv'
t20_df = pd.read_csv(csv_in, index_col=None, header=0, sep=',', encoding='utf-8')
In [9]:
t20_df.head(3)
Out[9]:
    id  created_at  user_id     user_name   tweet_text  htag    urls    ptxt    tgrp    date    user_handles    txt_terms   search_grp  lat     lon
0   638818911773856000  Tue Sep 01 21:01:11 +0000 2015  2511247075  Noor Din    RT @kdnuggets: R leads RapidMiner, Python catc...   [#KDN]  [://t.co/3bsaTT7eUs]    r leads rapidminer python catches up big data ...   [spark, python]     2015-09-01 21:01:11     [@kdnuggets]    r leads rapidminer python catches up big data ...   [spark, python]     37.279518   -121.867905
1   622142176768737000  Fri Jul 17 20:33:48 +0000 2015  24537879    IBM Cloudant    Be one of the first to sign-up for IBM Analyti...   [#ApacheSpark, #SparkInsight]   [://t.co/C5TZpetVA6, ://t.co/R1L29DePaQ]    be one of the first to sign up for ibm analyti...   [spark]     2015-07-17 20:33:48     []  be one of the first to sign up for ibm analyti...   [spark]     37.774930   -122.419420
2   622140453069169000  Fri Jul 17 20:26:57 +0000 2015  515145898   Arno Candel     Nice article on #apachespark, #hadoop and #dat...   [#apachespark, #hadoop, #datascience]   [://t.co/IyF44pV0f3]    nice article on apachespark hadoop and datasci...   [spark]     2015-07-17 20:26:57     [@h2oai]    nice article on apachespark hadoop and datasci...   [spark]     51.500130   -0.126305
In [98]:
len(t20_df.user_id.unique())
Out[98]:
19
In [17]:
t20_geo = t20_df[['date', 'lat', 'lon', 'user_name', 'tweet_text']]
In [24]:
# 
t20_geo.rename(columns={'user_name':'user', 'tweet_text':'text' }, inplace=True)
In [25]:
t20_geo.head(4)
Out[25]:
    date    lat     lon     user    text
0   2015-09-01 21:01:11     37.279518   -121.867905     Noor Din    RT @kdnuggets: R leads RapidMiner, Python catc...
1   2015-07-17 20:33:48     37.774930   -122.419420     IBM Cloudant    Be one of the first to sign-up for IBM Analyti...
2   2015-07-17 20:26:57     51.500130   -0.126305   Arno Candel     Nice article on #apachespark, #hadoop and #dat...
3   2015-07-17 19:35:31     51.500130   -0.126305   Ira Michael Blonder     Spark 101: Running Spark and #MapReduce togeth...
In [22]:
df = t20_geo
#
#
#

In [29]:
#
# Bokeh Visualization of tweets on world map
#
from bokeh.plotting import *
from bokeh.models import HoverTool, ColumnDataSource
from collections import OrderedDict
​
# Output in Jupyter Notebook
output_notebook()
​
# Get the world map
world_countries = data.copy()
​
# Get the tweet data
tweets_source = ColumnDataSource(df)
​
# Create world map 
countries_source = ColumnDataSource(data= dict(
    countries_xs=[world_countries[code]['lons'] for code in world_countries],
    countries_ys=[world_countries[code]['lats'] for code in world_countries],
    country = [world_countries[code]['name'] for code in world_countries],
))
​
# Instantiate the bokeh interactive tools 
TOOLS="pan,wheel_zoom,box_zoom,reset,resize,hover,save"
​
# Instantiate the figure object
p = figure(
    title="%s tweets " %(str(len(df.index))),
    title_text_font_size="20pt",
    plot_width=1000,
    plot_height=600,
    tools=TOOLS)
​
# Create world patches background
p.patches(xs="countries_xs", ys="countries_ys", source = countries_source, fill_color="#F1EEF6", fill_alpha=0.3,
        line_color="#999999", line_width=0.5)
​
​# Scatter plots by longitude and latitude
p.scatter(x="lon", y="lat", source=tweets_source, fill_color="#FF0000", line_color="#FF0000")
# 
​
​# Activate hover tool with user and corresponding tweet information
hover = p.select(dict(type=HoverTool))
hover.point_policy = "follow_mouse"
hover.tooltips = OrderedDict([
    ("user", "@user"),
   ("tweet", "@text"),
])
​
# Render the figure on the browser
show(p)
BokehJS successfully loaded.
    
inspect
    
#
#
#
​
In [ ]:
#
# Bokeh Google Map Visualization of London with hover on specific points
#
#
from __future__ import print_function

from bokeh.browserlib import view
from bokeh.document import Document
from bokeh.embed import file_html
from bokeh.models.glyphs import Circle
from bokeh.models import (
    GMapPlot, Range1d, ColumnDataSource,
    PanTool, WheelZoomTool, BoxSelectTool,
    HoverTool, ResetTool,
    BoxSelectionOverlay, GMapOptions)
from bokeh.resources import INLINE

x_range = Range1d()
y_range = Range1d()

# JSON style string taken from: https://snazzymaps.com/style/1/pale-dawn
map_options = GMapOptions(lat=51.50013, lng=-0.126305, map_type="roadmap", zoom=13, styles="""
[{"featureType":"administrative","elementType":"all","stylers":[{"visibility":"on"},{"lightness":33}]},
 {"featureType":"landscape","elementType":"all","stylers":[{"color":"#f2e5d4"}]},
 {"featureType":"poi.park","elementType":"geometry","stylers":[{"color":"#c5dac6"}]},
 {"featureType":"poi.park","elementType":"labels","stylers":[{"visibility":"on"},{"lightness":20}]},
 {"featureType":"road","elementType":"all","stylers":[{"lightness":20}]},
 {"featureType":"road.highway","elementType":"geometry","stylers":[{"color":"#c5c6c6"}]},
 {"featureType":"road.arterial","elementType":"geometry","stylers":[{"color":"#e4d7c6"}]},
 {"featureType":"road.local","elementType":"geometry","stylers":[{"color":"#fbfaf7"}]},
 {"featureType":"water","elementType":"all","stylers":[{"visibility":"on"},{"color":"#acbcc9"}]}]
""")

# TOOLS="pan,wheel_zoom,box_zoom,reset,hover,save"
plot = GMapPlot(
    x_range=x_range, y_range=y_range,
    map_options=map_options,
    title="London Meetups"
)

source = ColumnDataSource(
    data=dict(
        lat=[51.49013, 51.50013, 51.51013],
        lon=[-0.130305, -0.126305, -0.120305],
        fill=['orange', 'blue', 'green'],
        name=['LondonDataScience', 'Spark', 'MachineLearning'],
        text=['Graph Data & Algorithms','Spark Internals','Deep Learning on Spark']
    )
)

circle = Circle(x="lon", y="lat", size=15, fill_color="fill", line_color=None)
plot.add_glyph(source, circle)

# TOOLS="pan,wheel_zoom,box_zoom,reset,hover,save"
pan = PanTool()
wheel_zoom = WheelZoomTool()
box_select = BoxSelectTool()
reset = ResetTool()
hover = HoverTool()
# save = SaveTool()

plot.add_tools(pan, wheel_zoom, box_select, reset, hover)
overlay = BoxSelectionOverlay(tool=box_select)
plot.add_layout(overlay)

hover = plot.select(dict(type=HoverTool))
hover.point_policy = "follow_mouse"
hover.tooltips = OrderedDict([
    ("Name", "@name"),
    ("Text", "@text"),
    ("(Long, Lat)", "(@lon, @lat)"),
])

show(plot)

