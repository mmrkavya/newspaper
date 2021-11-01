
from Preprocess import preprocess
from news_article import get_article
import pickle
model_path='./model/_model.pickle'


def predict(url):
    if url is not None:
        try:
            model = pickle.load(open(model_path,'rb'))
            article=preprocess(get_article(url))
        except Exception as e:
            print("exception occured while parsing ")
            print(e)
            try:
                article = preprocess(url)
            except Exception as e1:
                return {"err": [e,e1]}
    
    output = {"category": (model.predict([article,]))[0]}
    print(output)
    return output
