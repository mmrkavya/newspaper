import uvicorn
from fastapi import FastAPI


# defining the main app
app = FastAPI(title="test", docs_url="/")


# Route definitions
@app.get("/ping")
# Healthcheck route to ensure that the API is up and running
def ping():
    return {"ping": "pong"}

#Naive Bayes Classifier


# Main function to start the app when main.py is called
if __name__ == "__main__":
    # Uvicorn is used to run the server and listen for incoming API requests on 0.0.0.0:8888
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)