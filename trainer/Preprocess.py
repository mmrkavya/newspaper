import nltk
import re


from tensorflow import keras
from keras.preprocessing.text import text_to_word_sequence
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
#import translate
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
#from nltk.stem.porter import PorterStemmer
import string
import pandas as pd

from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()
from nltk.corpus import wordnet
nltk.download('averaged_perceptron_tagger')
import pickle


def get_wordnet_pos(word):
    """Map POS tag to first character lemmatize() accepts"""
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {'j': wordnet.ADJ,
          'n': wordnet.NOUN,
          'v': wordnet.VERB,
          'r': wordnet.ADV}

    return tag_dict.get(tag, wordnet.NOUN)
  

#to lower case

#remove punctuations
def remove_punctuation(text):
   # res = re.sub(r'[^\w\s]', '', text)
   translator = str.maketrans(' ', ' ', string.punctuation)
   return text.translate(translator)
    



def preprocess(text): 
    article=text.lower()
    article=remove_punctuation(str(article))
    #removing numbers
    article = re.findall(r'[A-Za-z]+', article)
    tokens=str()
    #stop words list
    stop_words = set(stopwords.words("english"))
    for t in article:
       t2=nltk.word_tokenize(t)
       for i in t2:
         #removing duplicate words and stop words
         #if i not in tokens and i not in stop_words:
         if i not in stop_words:
           #tokens=tokens+[stemmer.stem(i),]
           #storing the words after lemmatizier
           tokens=tokens+" " +lemmatizer.lemmatize(i, pos ='v')
    #print(tokens)
    return tokens
print(preprocess("test test running"))