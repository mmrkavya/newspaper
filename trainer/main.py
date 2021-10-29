import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import model_training 
from typing import List
import datetime



# defining the main app
app = FastAPI(title="newspaper", docs_url="/")

# calling the load_model during startup.
# this will train the model and keep it loaded for prediction.
app.add_event_handler("startup", model_training.initial_model_training)

# class which is expected in the payload while training
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


# Route definitions
@app.get("/ping")
# Healthcheck route to ensure that the API is up and running
def ping():
    return {"ping": "pong"}


@app.post("/retrain", status_code=200)
# Route to further train the model based on user input in form of feedback loop
# Payload: FeedbackIn containing the parameters and correct flower class
# Response: Dict with detail confirming success (200)
def retrain(data: List[FeedbackIn]):
    model_training.retrain(data)
    # tell predictr to reload the model
    return {"detail": "Training successful"}

@app.get("/predict", status_code=200)
# Route to further train the model based on user input in form of feedback loop
# Payload: FeedbackIn containing the parameters and correct flower class
# Response: Dict with detail confirming success (200)
def predict(url: str):
    output=model_training.predict(url)
    output={"category": output['category'],'timestamp': datetime.datetime.now()}
    
    # tell predictr to reload the model
    return output


# Main function to start the app when main.py is called
if __name__ == "__main__":
    # Uvicorn is used to run the server and listen for incoming API requests on 0.0.0.0:8888
    uvicorn.run("main:app", host="0.0.0.0", port=7777, reload=True)