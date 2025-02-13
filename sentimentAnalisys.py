import pandas as pd
from textblob import TextBlob
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
import  seaborn as sns

# pd.set_option("display.max_colwidth",None)
print("started")
# nltk.download("stopwords")
# nltk.download("punkt")
# nltk.download("wordnet")

df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//reviews.csv")
target_pathcsv="C://Users//arsha//Desktop//HW//___projects____//outputresource//sentiment//spotify.csv"

reviews=df["Review"]

stop_word=stopwords.words("english")
lemmatizer = WordNetLemmatizer()
newlist=[]
for review in reviews:
    
    #lower case
    review=review.lower()
    #remove panctuation
    review = re.sub(r"[^a-zA-Z0-9\s]", "", review)

    #tokenization
    tokens=word_tokenize(review)
    
    tokens = [word for word in tokens if word not in stop_word]
    
    tokens = [lemmatizer.lemmatize(word) for word in tokens]
    newlist.append(tokens)

pollist=[]
for sublist in newlist:
    string=" ".join(sublist)
    blob=TextBlob(string)
    pol=blob.sentiment.polarity
    pollist.append(pol)

roundpolarity=[]

for item in pollist:
    roundpolarity.append(round(item,2))
    





#visualization
newpollist=[]
pollistlength=len(pollist)
for count in range(pollistlength):
    newpollist.append(count+1)

polaritylist=[newpollist,roundpolarity] 
    
plt.bar(polaritylist[0],polaritylist[1])
plt.title("review polarity")
plt.xlabel("polarity")
plt.ylabel("reviewCount")
plt.show()
