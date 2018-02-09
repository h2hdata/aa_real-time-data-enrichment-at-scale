import sys
import re
import time
import json
import re
import operator

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# FOR NER
import nltk
from nltk import sent_tokenize
from nltk import word_tokenize
from nltk import pos_tag
from nltk import ne_chunk
from nltk import tree
from geotext import GeoText

from nltk.corpus import stopwords as stopwords_nltk
from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer
import pandas as pd


"""
 DATA MUNGING
---------------
"""

class NerCore:
	def __init__(self, sentence):
		self.sentence = sentence

	def __preprocessing(self, lang):
		sentences = sent_tokenize(self.sentence)
		sentences = [word_tokenize(sent) for sent in sentences]
		sentences = [pos_tag(sent, lang=lang) for sent in sentences]
		return sentences

	def extract_names(self, lang):
		names = []
		sentences = self.__preprocessing(lang)
		for tagged_sentence in sentences:
			for chunk in ne_chunk(tagged_sentence):
				if isinstance(chunk, tree.Tree):
					if chunk.label() == 'PERSON':
						names.append(' '.join([c[0] for c in chunk]))
		return names

	def extract_location(self, lang):
		locations = []
		sentences = self.__preprocessing(lang)
		for tagged_sentence in sentences:
			for chunk in ne_chunk(tagged_sentence):
				if isinstance(chunk, tree.Tree):
					if chunk.label == 'LOCATION':
						locations.append(' '.join([c[0] for c in chunk]))

		cities = GeoText(self.sentence).cities
		for city in cities:
			locations.append(''.join([c[0] for c in city]))
		nationalities = GeoText(self.sentence).nationalities
		for national in nationalities:
			locations.append(''.join([n[0] for n in national]))
		countries = GeoText(self.sentence).countries
		for country in countries:
			locations.append(''.join([c[0] for c in country]))
		return locations

	def extract_date_time(self):
		dateTimes = []
		grammar = r"DATE: {<NNP><CD>}"
		parser = nltk.RegexpParser(grammar)

		phrase_tagger = nltk.pos_tag(word_tokenize(self.sentence))
		phrase_chunk = nltk.ne_chunk(phrase_tagger)

		phrase_date = parser.parse(phrase_chunk)
		for word in phrase_date:
			if isinstance(word, nltk.tree.Tree) and word.label() == 'DATE':
				dateTimes.append(' '.join([w[0] for w in word]))
		return dateTimes


# Starting the spark context
# SparkContext('local[1]') would not work with Streaming bc 2 threads are required
sc = SparkContext("local[2]", "Twitter Demo")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10) #10 is the batch interval in seconds
IP = "localhost"
Port = 5556
lines = ssc.socketTextStream(IP, Port)
 
"""
 DATA ENRICHMENT
-----------------
"""
def searchGeolocation(sentence):

	reg = '(geo=\()( )*[0-9]+( )*,( )*[0-9]+( )*\)'

	m = re.search(reg,sentence)

	if m:
		found = m.group(0)

		out = found[5:-1].split(',')

		req = [int(i) for i in out ]

		return req

	else:

		return 0

def stateSearch(sentence):

	# reg = '([A-Z]{2}( )+)'
	reg = '([A-Z]{2})'

	m = re.search(reg,sentence)

	print m

	# exit()

	if m:
		found = m.group(0)

		return found

	else:

		return 0

def zipcodeExtracter(sentence):

	reg = re.compile('^.*(?P<zipcode>\d{5}).*$')

	try:
		match = reg.match(sentence)

		return match.groupdict()['zipcode']
	except:

		return 0

def get_result(sentence):
	out = []
	for word in sentence:
		out.append(word)
	return out

def map_tweets(tweet):

	json_tweet = json.loads(tweet)
	if json_tweet.has_key('lang'): # When the lang key was not present it caused issues
		if json_tweet['lang'] == 'en':
			tweet = json_tweet['text']
			# user_mentionList_dict1 = json_tweet['entities']
			print "\n\n\n\n"
			print json_tweet
			print "\n\n\n\n"

			print "*************************************************************************************"

			print "\n\n\n\n"
			# print user_mentionList_dict1,'\n\njfyhdhgdhfhnuyjfutdjutfuktimymo,yiomgymkhkhgjkgjfhjfjgkjgk,k,hk,h\n\n\n\n\n'
			# user_mentionList_dict2 = user_mentionList_dict1['user_mentions'][0]
			name = json_tweet['user']['name']
			userid = json_tweet['user']['id']
			import pandas as pd
			referance = pd.read_csv('../Case/us_codes.csv')

			statelist = referance['State'].tolist()

			stateabbrv = referance['State Abbreviation'].tolist()

			countrylist = referance['County'].tolist()

			placelist = referance['Place Name'].tolist()

			ziplist = referance['Zip Code'].tolist()

			latlist = referance['Latitude'].tolist()

			longlist = referance['Longitude'].tolist()
			state=None
			try:
				out = searchGeolocation(tweet)

				if out != 0:

					lat = out[0]
					longt = out[1]

					if lat in latlist and longt in longlist:

						index = latlist.index(lat)

						state = statelist[index]

			except:

				state = None

			tokenizer = RegexpTokenizer(r'\w+')

			sentence = tokenizer.tokenize(tweet)

			sentence = ' '.join(sentence)

			good_sentence = sentence # Use this for Symatic Analysis

			try:

				outer = stateSearch(sentence)

				if outer != 0:

					if outer in stateabbrv:

						index = stateabbrv.index(outer)

						state = statelist[index]
			except:

				state = None
		
			try:

				ner = NerCore(sentence)

				# names = ner.extract_names("eng")
				locations = ner.extract_location("eng")

				req = get_result(locations)

				if len(req) > 0:

					loc = max(set(req),keys=lst.count)

					if loc in countrylist:

						index = countrylist.index(loc)

						state = statelist[index] 

					elif loc in placelist:

						index = placelist.index(loc)

						state = statelist[index]

			except:

				state = None

			try:

				outer = str(zipcodeExtracter(sentence))

				if  outer!= '0':

						ziplist = map(lambda x: str(x),ziplist)

						if outer in ziplist:

							index = ziplist.index(outer)


							state = statelist[index]
			except:
				state = None
			
			if state is None:
				state= '-----'

			import pandas as pd

			#Semantic Hobby Search
			hobby_referance = pd.read_csv('../Case/Hobbies.csv')
			hobbieslist = hobby_referance['Hobbies'].tolist()

			from stemming.porter2 import stem

			sentence = [stem(word) for word in sentence.split(' ')]
			hobbieslist = [stem(word) for word in hobbieslist]

			hobbieslist = list(set(sentence).intersection(set(hobbieslist)))

			if hobbieslist:
				return [str(userid), str(state), str(hobbieslist).strip('[').strip(']')]
			
			return [str(userid), str(state), '-------']


# When your DStream in Spark receives data, it creates an RDD every batch interval.
# We use coalesce(1) to be sure that the final filtered RDD has only one partition,
# so that we have only one resulting part-00000 file in the directory.
# The method saveAsTextFile() should really be re-named saveInDirectory(),
# because that is the name of the directory in which the final part-00000 file is saved.
# We use time.time() to make sure there is always a newly created directory, otherwise
# it will throw an Exception

lines.foreachRDD( lambda rdd: rdd.map(map_tweets).coalesce(1).saveAsTextFile('../Output/Raw_output/'+str(time.time())) )
 
# We must start the Spark StreamingContext, and await process termination
ssc.start()
ssc.awaitTermination()
