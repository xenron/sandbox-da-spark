##
#
# Spark for Python - Chapter 4 - Code
#
##

#
# Local Vector in PySpark
#
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors

# NumPy array for dense vector.
dvect1 = np.array([5.0, 0.0, 1.0, 7.0])
# Python list for dense vector.
dvect2 = [5.0, 0.0, 1.0, 7.0]
# SparseVector creation
svect1 = Vectors.sparse(4, [0, 2, 3], [5.0, 1.0, 7.0])
# Sparse vector using a single-column SciPy csc_matrix
svect2 = sps.csc_matrix((np.array([5.0, 1.0, 7.0]), np.array([0, 2, 3])), shape = (4, 1))


#
# Labelled Point in PySpark
#
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Labelled point with a positive label and a dense feature vector.
lp_pos = LabeledPoint(1.0, [5.0, 0.0, 1.0, 7.0])

# Labelled point with a negative label and a sparse feature vector.
lp_neg = LabeledPoint(0.0, SparseVector(4, [0, 2, 3], [5.0, 1.0, 7.0]))

#
# Local Matrix
#
from pyspark.mllib.linalg import Matrix, Matrices

# Dense matrix ((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
dMatrix = Matrices.dense(2, 3, [1, 2, 3, 4, 5, 6])

# Sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
sMatrix = Matrices.sparse(3, 2, [0, 1, 3], [0, 2, 1], [9, 6, 8])

#
# Code Plan
#
#
# 1- Combine all tweets files into a single data frame
# 2- Parse the Tweets - remove stopwords - extract emoticons - extract url - normalize your words (e.g., mapping them to lowercase and removing punctuation and numbers)
# 3- Feature extraction
# 		3a- Tokenisation
# 		3b- TF-IDF
# 		3c- Hash TF-IDF
# 4- Run K-Means clustering
# 5- Evaluate
# 		5a- Identify Tweets to Cluster
# 		5b- Dimemsionality reduction to 2 dim with PCA
# 		5c- Plotting the clusters 
# 6- Pipeline
#

#
# KMeans Clustering using Python SK-Learn SciKit-Learn 
#

#
# Loading the data
#
In [19]:

import pandas as pd
In [22]:

csv_in = 'C:\\Users\\Amit\\Documents\\IPython Notebooks\\AN00_Data\\unq_tweetstxt.csv'
twts_df01 = pd.read_csv(csv_in, sep =';', encoding='utf-8')

In [24]:

twts_df01.count()
Out[24]:
Unnamed: 0    7540
id            7540
created_at    7540
user_id       7540
user_name     7538
tweet_text    7540
dtype: int64

#
# Introspeccting the tweets text 
#
In [82]:

twtstxt_ls01[6910:6920]
Out[82]:
['RT @deroach_Ismoke: I am NOT voting for #hilaryclinton http://t.co/jaZZpcHkkJ',
 'RT @AnimalRightsJen: #HilaryClinton What do Bernie Sanders and Donald Trump Have in Common?: He has so far been th... http://t.co/t2YRcGCh6…',
 'I understand why Bill was out banging other chicks........I mean look at what he is married to.....\n@HilaryClinton',
 '#HilaryClinton What do Bernie Sanders and Donald Trump Have in Common?: He has so far been th... http://t.co/t2YRcGCh67 #Tcot #UniteBlue']

#
# Feature extraction from tweets text using a sparse vectorizer
# Using TF-IDF vectorizer with 10,000 features, and English stop words
#

In [37]:

print("Extracting features from the training dataset using a sparse vectorizer")
t0 = time()
Extracting features from the training dataset using a sparse vectorizer
In [38]:

vectorizer = TfidfVectorizer(max_df=0.5, max_features=10000,
                                 min_df=2, stop_words='english',
                                 use_idf=True)
X = vectorizer.fit_transform(twtstxt_ls01)
#
#
#
print("done in %fs" % (time() - t0))
print("n_samples: %d, n_features: %d" % X.shape)
print()
done in 5.232165s
n_samples: 7540, n_features: 6638

#
# KMeans Clustering
# Initial clusters = 7
# Maximum iteration = 100
#

In [47]:

km = KMeans(n_clusters=7, init='k-means++', max_iter=100, n_init=1,
            verbose=1)

print("Clustering sparse data with %s" % km)
t0 = time()
km.fit(X)
print("done in %0.3fs" % (time() - t0))

Clustering sparse data with KMeans(copy_x=True, init='k-means++', max_iter=100, n_clusters=7, n_init=1,
    n_jobs=1, precompute_distances='auto', random_state=None, tol=0.0001,
    verbose=1)
Initialization complete
Iteration  0, inertia 13635.141
Iteration  1, inertia 6943.485
Iteration  2, inertia 6924.093
Iteration  3, inertia 6915.004
Iteration  4, inertia 6909.212
Iteration  5, inertia 6903.848
Iteration  6, inertia 6888.606
Iteration  7, inertia 6863.226
Iteration  8, inertia 6860.026
Iteration  9, inertia 6859.338
Iteration 10, inertia 6859.213
Iteration 11, inertia 6859.102
Iteration 12, inertia 6859.080
Iteration 13, inertia 6859.060
Iteration 14, inertia 6859.047
Iteration 15, inertia 6859.039
Iteration 16, inertia 6859.032
Iteration 17, inertia 6859.031
Iteration 18, inertia 6859.029
Converged at iteration 18
done in 1.701s

#
# Introspect top terms per cluster
#

In [49]:

print("Top terms per cluster:")
order_centroids = km.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()
for i in range(7):
    print("Cluster %d:" % i, end='')
    for ind in order_centroids[i, :20]:
        print(' %s' % terms[ind], end='')
    print()
Top terms per cluster:
Cluster 0: justinbieber love mean rt follow thank hi https whatdoyoumean video wanna hear whatdoyoumeanviral rorykramer happy lol making person dream justin
Cluster 1: donaldtrump hilaryclinton rt https trump2016 realdonaldtrump trump gop amp justinbieber president clinton emails oy8ltkstze tcot like berniesanders hilary people email
Cluster 2: bigdata apachespark hadoop analytics rt spark training chennai ibm datascience apache processing cloudera mapreduce data sap https vora transforming development
Cluster 3: apachespark python https rt spark data amp databricks using new learn hadoop ibm big apache continuumio bluemix learning join open
Cluster 4: ernestsgantt simbata3 jdhm2015 elsahel12 phuketdailynews dreamintentions beyhiveinfrance almtorta18 civipartnership 9_a_6 25whu72ep0 k7erhvu7wn fdmxxxcm3h osxuh2fxnt 5o5rmb0xhp jnbgkqn0dj ovap57ujdh dtzsz3lb6x sunnysai12345 sdcvulih6g
Cluster 5: trump donald donaldtrump starbucks trumpquote trumpforpresident oy8ltkstze https zfns7pxysx silly goy stump trump2016 news jeremy coffee corbyn ok7vc8aetz rt tonight
Cluster 6: ladygaga gaga lady rt https love follow horror cd story ahshotel american japan hotel humantrafficking music fashion diet queen ahs

#
# Plotting the clusters
# Using Multi Dimensional Scaling DMS to bring the multi dimensional feature clusters into  2 dimensions to be able to visualize them
#

In [73]:
import matplotlib.pyplot as plt
import matplotlib as mpl
from sklearn.manifold import MDS

MDS()

#
# Bring down the MDS to two dimensions (components) as we will plot the clusters
#
mds = MDS(n_components=2, dissimilarity="precomputed", random_state=1)

pos = mds.fit_transform(dist)  # shape (n_components, n_samples)

xs, ys = pos[:, 0], pos[:, 1]

In [67]:

#
# Set up colors per clusters using a dict
#
cluster_colors = {0: '#1b9e77', 1: '#d95f02', 2: '#7570b3', 3: '#e7298a', 4: '#66a61e', 5: '#9990b3', 6: '#e8888a'}

#
#set up cluster names using a dict
#
cluster_names = {0: 'Music, Pop', 
                 1: 'Politics, Election', 
                 2: 'BigData, Spark', 
                 3: 'Spark, Python',
                 4: 'Thailand', 
                 5: 'Politics, Election', 
                 6: 'Music, Pop'}
In [115]:
#
# ipython magic to show the matplotlib plots inline
#
%matplotlib inline 

#
# Create data frame which includes MDS results, cluster numbers and tweet texts to be displayed
#
df = pd.DataFrame(dict(x=xs, y=ys, label=clusters, txt=twtstxt_ls02_utf8))
ix_start = 2000
ix_stop  = 2050
df01 = df[ix_start:ix_stop]

print(df01[['label','txt']])
print(len(df01))
print()

#
# Group by cluster
#
groups = df.groupby('label')
groups01 = df01.groupby('label')

#
# Set up the plot
#
fig, ax = plt.subplots(figsize=(17, 10)) 
ax.margins(0.05) 

#
# Build the plot object
#
for name, group in groups01:
    ax.plot(group.x, group.y, marker='o', linestyle='', ms=12, 
            label=cluster_names[name], color=cluster_colors[name], 
            mec='none')
    ax.set_aspect('auto')
    ax.tick_params(\
        axis= 'x',         # settings for x-axis
        which='both',      # 
        bottom='off',      # 
        top='off',         # 
        labelbottom='off')
    ax.tick_params(\
        axis= 'y',         # settings for y-axis
        which='both',      # 
        left='off',        # 
        top='off',         # 
        labelleft='off')
    
ax.legend(numpoints=1)     #
#
# Add label in x,y position with tweet text
#
for i in range(ix_start, ix_stop):
    ax.text(df01.ix[i]['x'], df01.ix[i]['y'], df01.ix[i]['txt'], size=10)  
    
plt.show()                 # Display the plot


      label       text
2000      2       b'RT @BigDataTechCon: '
2001      3       b"@4Quant 's presentat"
2002      2       b'Cassandra Summit 201'

###
#
# Apche Spark Feature Extraction, KMeans
#
###

In [3]:
#
# Read csv in a Panda DF
#
#
import pandas as pd
csv_in = '/home/an/spark/spark-1.5.0-bin-hadoop2.6/examples/AN_Spark/data/unq_tweetstxt.csv'
pddf_in = pd.read_csv(csv_in, index_col=None, header=0, sep=';', encoding='utf-8')

In [4]:

sqlContext = SQLContext(sc)

In [5]:

#
# Convert a Panda DF to a Spark DF
#
#

spdf_02 = sqlContext.createDataFrame(pddf_in[['id', 'user_id', 'user_name', 'tweet_text']])

In [8]:

spdf_02.show()

In [7]:

spdf_02.take(3)

Out[7]:

[Row(id=638830426971181057, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: 9_A_6: dreamintentions: elsahel12: simbata3: JDHM2015: almtorta18: dreamintentions:\u2026 http://t.co/VpD7FoqMr0'),
 Row(id=638830426727911424, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: PhuketDailyNews: dreamintentions: elsahel12: simbata3: JDHM2015: almtorta18: CiviPa\u2026 http://t.co/VpD7FoqMr0'),
 Row(id=638830425402556417, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: 9_A_6: ernestsgantt: elsahel12: simbata3: JDHM2015: almtorta18: CiviPartnership: dr\u2026 http://t.co/EMDOn8chPK')]

In [9]:

from pyspark.ml.feature import HashingTF, IDF, Tokenizer

In [10]:

#
# Tokenize the tweet_text 
#
tokenizer = Tokenizer(inputCol="tweet_text", outputCol="tokens")
tokensData = tokenizer.transform(spdf_02)

In [11]:

tokensData.take(1)

Out[11]:

[Row(id=638830426971181057, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: 9_A_6: dreamintentions: elsahel12: simbata3: JDHM2015: almtorta18: dreamintentions:\u2026 http://t.co/VpD7FoqMr0', tokens=[u'ernestsgantt:', u'beyhiveinfrance:', u'9_a_6:', u'dreamintentions:', u'elsahel12:', u'simbata3:', u'jdhm2015:', u'almtorta18:', u'dreamintentions:\u2026', u'http://t.co/vpd7foqmr0'])]

In [14]:

#
# Apply Hashing TF to the tokens
#
hashingTF = HashingTF(inputCol="tokens", outputCol="rawFeatures", numFeatures=2000)
featuresData = hashingTF.transform(tokensData)

In [15]:

featuresData.take(1)

Out[15]:

[Row(id=638830426971181057, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: 9_A_6: dreamintentions: elsahel12: simbata3: JDHM2015: almtorta18: dreamintentions:\u2026 http://t.co/VpD7FoqMr0', tokens=[u'ernestsgantt:', u'beyhiveinfrance:', u'9_a_6:', u'dreamintentions:', u'elsahel12:', u'simbata3:', u'jdhm2015:', u'almtorta18:', u'dreamintentions:\u2026', u'http://t.co/vpd7foqmr0'], rawFeatures=SparseVector(2000, {74: 1.0, 97: 1.0, 100: 1.0, 160: 1.0, 185: 1.0, 742: 1.0, 856: 1.0, 991: 1.0, 1383: 1.0, 1620: 1.0}))]

In [16]:

#
# Apply IDF to the raw features and rescale the data
#
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featuresData)
rescaledData = idfModel.transform(featuresData)

for features in rescaledData.select("features").take(3):
  print(features)

In [17]:

rescaledData.take(2)

Out[17]:

[Row(id=638830426971181057, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: 9_A_6: dreamintentions: elsahel12: simbata3: JDHM2015: almtorta18: dreamintentions:\u2026 http://t.co/VpD7FoqMr0', tokens=[u'ernestsgantt:', u'beyhiveinfrance:', u'9_a_6:', u'dreamintentions:', u'elsahel12:', u'simbata3:', u'jdhm2015:', u'almtorta18:', u'dreamintentions:\u2026', u'http://t.co/vpd7foqmr0'], rawFeatures=SparseVector(2000, {74: 1.0, 97: 1.0, 100: 1.0, 160: 1.0, 185: 1.0, 742: 1.0, 856: 1.0, 991: 1.0, 1383: 1.0, 1620: 1.0}), features=SparseVector(2000, {74: 2.6762, 97: 1.8625, 100: 2.6384, 160: 2.9985, 185: 2.7481, 742: 5.5269, 856: 4.1406, 991: 2.9518, 1383: 4.694, 1620: 3.073})),
 Row(id=638830426727911424, user_id=3276255125, user_name=u'True Equality', tweet_text=u'ernestsgantt: BeyHiveInFrance: PhuketDailyNews: dreamintentions: elsahel12: simbata3: JDHM2015: almtorta18: CiviPa\u2026 http://t.co/VpD7FoqMr0', tokens=[u'ernestsgantt:', u'beyhiveinfrance:', u'phuketdailynews:', u'dreamintentions:', u'elsahel12:', u'simbata3:', u'jdhm2015:', u'almtorta18:', u'civipa\u2026', u'http://t.co/vpd7foqmr0'], rawFeatures=SparseVector(2000, {74: 1.0, 97: 1.0, 100: 1.0, 160: 1.0, 185: 1.0, 460: 1.0, 987: 1.0, 991: 1.0, 1383: 1.0, 1620: 1.0}), features=SparseVector(2000, {74: 2.6762, 97: 1.8625, 100: 2.6384, 160: 2.9985, 185: 2.7481, 460: 6.4432, 987: 2.9959, 991: 2.9518, 1383: 4.694, 1620: 3.073}))]

In [21]:

rs_pddf = rescaledData.toPandas()

In [22]:

rs_pddf.count()

Out[22]:

id             7540
user_id        7540
user_name      7540
tweet_text     7540
tokens         7540
rawFeatures    7540
features       7540
dtype: int64


In [27]:

feat_lst = rs_pddf.features.tolist()

In [28]:

feat_lst[:2]

Out[28]:

[SparseVector(2000, {74: 2.6762, 97: 1.8625, 100: 2.6384, 160: 2.9985, 185: 2.7481, 742: 5.5269, 856: 4.1406, 991: 2.9518, 1383: 4.694, 1620: 3.073}),
 SparseVector(2000, {74: 2.6762, 97: 1.8625, 100: 2.6384, 160: 2.9985, 185: 2.7481, 460: 6.4432, 987: 2.9959, 991: 2.9518, 1383: 4.694, 1620: 3.073})]

 In [32]:

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

In [34]:

# Load and parse the data


in_Data = sc.parallelize(feat_lst)

In [35]:

in_Data.take(3)

Out[35]:

[SparseVector(2000, {74: 2.6762, 97: 1.8625, 100: 2.6384, 160: 2.9985, 185: 2.7481, 742: 5.5269, 856: 4.1406, 991: 2.9518, 1383: 4.694, 1620: 3.073}),
 SparseVector(2000, {74: 2.6762, 97: 1.8625, 100: 2.6384, 160: 2.9985, 185: 2.7481, 460: 6.4432, 987: 2.9959, 991: 2.9518, 1383: 4.694, 1620: 3.073}),
 SparseVector(2000, {20: 4.3534, 74: 2.6762, 97: 1.8625, 100: 5.2768, 185: 2.7481, 856: 4.1406, 991: 2.9518, 1039: 3.073, 1620: 3.073, 1864: 4.6377})]

In [37]:

in_Data.count()

Out[37]:

7540

In [38]:

# Build the model (cluster the data)

clusters = KMeans.train(in_Data, 5, maxIterations=10,
        runs=10, initializationMode="random")

In [53]:

# Evaluate clustering by computing Within Set Sum of Squared Errors

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


WSSSE = in_Data.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Evaluating the Model and the Results
# One way to fine tune the clustering algorithm is varying the number of clusters and verifying the output. 
# We will introspect the clusters and get a feel for the clustering results so far. 
In [43]:

cluster_membership = in_Data.map(lambda x: clusters.predict(x))

In [54]:

cluster_idx = cluster_membership.zipWithIndex()

In [55]:

type(cluster_idx)

Out[55]:

pyspark.rdd.PipelinedRDD

In [58]:

cluster_idx.take(20)

Out[58]:

[(3, 0),
 (3, 1),
 (3, 2),
 (3, 3),
 (3, 4),
 (3, 5),
 (1, 6),
 (3, 7),
 (3, 8),
 (3, 9),
 (3, 10),
 (3, 11),
 (3, 12),
 (3, 13),
 (3, 14),
 (1, 15),
 (3, 16),
 (3, 17),
 (1, 18),
 (1, 19)]

In [59]:

cluster_df = cluster_idx.toDF()

In [65]:

pddf_with_cluster = pd.concat([pddf_in, cluster_pddf],axis=1)

In [76]:

pddf_with_cluster._1.unique()

Out[76]:

array([3, 1, 4, 0, 2])

In [79]:

pddf_with_cluster[pddf_with_cluster['_1'] == 0].head(10)

Out[79]:
	Unnamed: 0 	id 	created_at 	user_id 	user_name 	tweet_text 	_1 	_2
6227 	3 	642418116819988480 	Fri Sep 11 19:23:09 +0000 2015 	49693598 	Ajinkya Kale 	RT @bigdata: Distributed Matrix Computations i... 	0 	6227
6257 	45 	642391207205859328 	Fri Sep 11 17:36:13 +0000 2015 	937467860 	Angela Bassa 	[Auto] I'm reading ""Distributed Matrix Comput... 	0 	6257
6297 	119 	642348577147064320 	Fri Sep 11 14:46:49 +0000 2015 	18318677 	Ben Lorica 	Distributed Matrix Computations in @ApacheSpar... 	0 	6297
In [80]:

pddf_with_cluster[pddf_with_cluster['_1'] == 1].head(10)

Out[80]:
	Unnamed: 0 	id 	created_at 	user_id 	user_name 	tweet_text 	_1 	_2
6 	6 	638830419090079746 	Tue Sep 01 21:46:55 +0000 2015 	2241040634 	Massimo Carrisi 	Python:Python: Removing \xa0 from string? - I ... 	1 	6
15 	17 	638830380578045953 	Tue Sep 01 21:46:46 +0000 2015 	57699376 	Rafael Monnerat 	RT @ramalhoorg: Noite de autógrafos do Fluent ... 	1 	15
18 	41 	638830280988426250 	Tue Sep 01 21:46:22 +0000 2015 	951081582 	Jack Baldwin 	RT @cloudaus: We are 3/4 full! 2-day @swcarpen... 	1 	18
19 	42 	638830276626399232 	Tue Sep 01 21:46:21 +0000 2015 	6525302 	Masayoshi Nakamura 	PynamoDB使いやすいです #AWS #DynamoDB #Python http://... 	1 	19
20 	43 	638830213288235008 	Tue Sep 01 21:46:06 +0000 2015 	3153874869 	Baltimore Python 	Flexx: Python UI tookit based on web technolog... 	1 	20
21 	44 	638830117645516800 	Tue Sep 01 21:45:43 +0000 2015 	48474625 	Radio Free Denali 	Hmm, emerge --depclean wants to remove somethi... 	1 	21
22 	46 	638829977014636544 	Tue Sep 01 21:45:10 +0000 2015 	154915461 	Luciano Ramalho ☂ 	Noite de autógrafos do Fluent Python no Garoa ... 	1 	22
23 	47 	638829882928070656 	Tue Sep 01 21:44:47 +0000 2015 	917320920 	bsbafflesbrains 	@DanSWright Harper channeling Monty Python. "... 	1 	23
24 	48 	638829868679954432 	Tue Sep 01 21:44:44 +0000 2015 	134280898 	Lannick Technology 	RT @SergeyKalnish: I am #hiring: Senior Back e... 	1 	24
25 	49 	638829707484508161 	Tue Sep 01 21:44:05 +0000 2015 	2839203454 	Joshua Jones 	RT @LindseyPelas: Surviving Monty Python in Fl... 	1 	25
In [81]:

pddf_with_cluster[pddf_with_cluster['_1'] == 2].head(10)

Out[81]:
	Unnamed: 0 	id 	created_at 	user_id 	user_name 	tweet_text 	_1 	_2
7280 	688 	639056941592014848 	Wed Sep 02 12:47:02 +0000 2015 	2735137484 	Chris 	A true gay icon when will @ladygaga @Madonna @... 	2 	7280
In [82]:

pddf_with_cluster[pddf_with_cluster['_1'] == 3].head(10)

Out[82]:
	Unnamed: 0 	id 	created_at 	user_id 	user_name 	tweet_text 	_1 	_2
0 	0 	638830426971181057 	Tue Sep 01 21:46:57 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: 9_A_6: dreamint... 	3 	0
1 	1 	638830426727911424 	Tue Sep 01 21:46:57 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews... 	3 	1
2 	2 	638830425402556417 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: 9_A_6: ernestsg... 	3 	2
3 	3 	638830424563716097 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews... 	3 	3
4 	4 	638830422256816132 	Tue Sep 01 21:46:56 +0000 2015 	3276255125 	True Equality 	ernestsgantt: elsahel12: 9_A_6: dreamintention... 	3 	4
5 	5 	638830420159655936 	Tue Sep 01 21:46:55 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews... 	3 	5
7 	7 	638830418330980352 	Tue Sep 01 21:46:55 +0000 2015 	3276255125 	True Equality 	ernestsgantt: elsahel12: 9_A_6: dreamintention... 	3 	7
8 	8 	638830397648822272 	Tue Sep 01 21:46:50 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews... 	3 	8
9 	9 	638830395375529984 	Tue Sep 01 21:46:49 +0000 2015 	3276255125 	True Equality 	ernestsgantt: elsahel12: 9_A_6: dreamintention... 	3 	9
10 	10 	638830392389177344 	Tue Sep 01 21:46:49 +0000 2015 	3276255125 	True Equality 	ernestsgantt: BeyHiveInFrance: PhuketDailyNews... 	3 	10
In [83]:

pddf_with_cluster[pddf_with_cluster['_1'] == 4].head(10)

Out[83]:
	Unnamed: 0 	id 	created_at 	user_id 	user_name 	tweet_text 	_1 	_2
1361 	882 	642648214454317056 	Sat Sep 12 10:37:28 +0000 2015 	27415756 	Raymond Enisuoh 	LA Chosen For US 2024 Olympic Bid - LA2016 See... 	4 	1361
1363 	885 	642647848744583168 	Sat Sep 12 10:36:01 +0000 2015 	27415756 	Raymond Enisuoh 	Prison See: https://t.co/x3EKAExeFi … … … … … ... 	4 	1363
5412 	11 	640480770369286144 	Sun Sep 06 11:04:49 +0000 2015 	3242403023 	Donald Trump 2016 	" igiboooy! 😀😀😀 @ Starbucks https://t.co/97wdL... 	4 	5412
5428 	27 	640477140660518912 	Sun Sep 06 10:50:24 +0000 2015 	3242403023 	Donald Trump 2016 	" 🙆 @ Starbucks https://t.co/wsEYFIefk7 " - D... 	4 	5428
5455 	61 	640469542272110592 	Sun Sep 06 10:20:12 +0000 2015 	3242403023 	Donald Trump 2016 	" starbucks @ Starbucks Mam Plaza https://t.co... 	4 	5455
5456 	62 	640469541370372096 	Sun Sep 06 10:20:12 +0000 2015 	3242403023 	Donald Trump 2016 	" Aaahhh the pumpkin spice latte is back, fall... 	4 	5456
5457 	63 	640469539524898817 	Sun Sep 06 10:20:12 +0000 2015 	3242403023 	Donald Trump 2016 	" RT kayyleighferry: Oh my goddd Harry Potter ... 	4 	5457
5458 	64 	640469537176031232 	Sun Sep 06 10:20:11 +0000 2015 	3242403023 	Donald Trump 2016 	" Starbucks https://t.co/3xYYXlwNkf " - Donald... 	4 	5458
5459 	65 	640469536119070720 	Sun Sep 06 10:20:11 +0000 2015 	3242403023 	Donald Trump 2016 	" A Starbucks is under construction in my neig... 	4 	5459
5460 	66 	640469530435813376 	Sun Sep 06 10:20:10 +0000 2015 	3242403023 	Donald Trump 2016 	" Babam starbucks'tan fotogtaf atıyor bende du... 	4 	5460
