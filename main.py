from fastapi import FastAPI
from pydantic import BaseModel
import os
from datetime import datetime
import dataQuality
import requests
import shutil

app = FastAPI()

class Request(BaseModel):
    body: str

@app.get("/")
async def root():
    return {"message": os.listdir(os.getcwd())}

@app.post("/start")
async def start_pipeline(args: Request):
    #writing raw data to bucket
    body = args.body
    if os.path.exists("data/raw")==False:
        os.mkdir("data/raw")
    datapath = os.path.join(os.getcwd(), "data/raw/raw_data_" + str(datetime.now()) + ".csv")
    with open(datapath, "w") as fp:
        fp.write(body)

    #preparing data quality folder
    if os.path.exists("data/data_quality")==False:
        os.mkdir("data/data_quality")
    dqpath = os.path.join(os.getcwd(), "data/data_quality/dq_log_" + str(datetime.now()) + ".txt")

    #starting data quality pipeline
    unique = dataQuality.uniqueness(datapath, dqpath)
    valid = dataQuality.validity(unique, dqpath)
    complete = dataQuality.completeness(valid, dqpath)

    #sending webhook
    with open(dqpath, "r") as logpath:
        url = os.environ["WEBHOOK_URL"]
        payload = {"text":logpath.read()}
        r = requests.post(url, json=payload)
    

@app.get("/check")
async def check_result():
    with open(os.path.join(os.getcwd(), "data/test.txt"), "r") as fp:
        text = fp.read()
        return {"message": text}