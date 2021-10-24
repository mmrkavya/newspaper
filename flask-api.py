import uvicorn
import datetime
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from news_article import get_article
from Preprocess import preprocess
import pickle
from model_training import retrain

# defining the main app
app = FastAPI(title="news predictor", docs_url="/")

# calling the load_model during startup.
# this will train the model and keep it loaded for prediction.


# class which is expected in the payload
class QueryIn(BaseModel):
     url: str

# class which is returned in the response
class QueryOut(BaseModel):
    category: str

# class which is expected in the payload while re-training
class FeedbackIn(BaseModel):
    source: str
    author: str
    title: str
    description: str
    url: str
    urlToImage: str
    publishedAt: str
    content: str
    category: str
    article: str
    summary: str
    
    
  


# class which is returned in the response for Feedback
class FeedbackOut(BaseModel):
    detail: str
    timestamp: datetime.datetime

# Route definitions
@app.get("/ping")
# Healthcheck route to ensure that the API is up and running
def ping():
    return {"ping": "pong"}

@app.post("/predict_news_title", response_model=QueryOut, status_code=200)
def predict_news_type(query_data: QueryIn):
    url=query_data.url
    if url is not None:
        try:
            model = pickle.load(open('_model.pickle','rb'))
            article=preprocess(get_article(url))
        except Exception as e:
            print("exception occured while parsing "+e)
            return {"err": e}
    output = {"category": (model.predict([article,]))[0],'timestamp': datetime.datetime.now()}
    return output


@app.post("/feedback_loop", response_model=FeedbackOut,status_code=200)
# Route to further train the model based on user input in form of feedback loop
# Payload: FeedbackIn containing the parameters and correct flower class
# Response: Dict with detail confirming success (200)
def feedback_loop(data: List[FeedbackIn]):
     retrain(dict(data))
     return {"detail": "Feedback loop successful",'timestamp': datetime.datetime.now()}


# Main function to start the app when main.py is called
if __name__ == "__main__":
    # Uvicorn is used to run the server and listen for incoming API requests on 0.0.0.0:8888
    uvicorn.run("main:app", host="0.0.0.0", port=8888, reload=True)