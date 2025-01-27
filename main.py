from fastapi import FastAPI
from pydantic import BaseModel
import os
from datetime import datetime
import dataQuality
import featureMining
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

    #running data quality pipeline
    complete = dataQuality.runDataQuality(datapath, dqpath)

    #preparing feature mining folder
    if os.path.exists("data/feature_mining")==False:
        os.mkdir("data/feature_mining")
    fmpath = os.path.join(os.getcwd(), "data/feature_mining/fm_log_" + str(datetime.now()) + ".txt")

    #running feature mining pipeline
    mined = featureMining.runFeatureMining(complete, fmpath)

@app.get("/check")
async def check_result():
    with open(os.path.join(os.getcwd(), "data/test.txt"), "r") as fp:
        text = fp.read()
        return {"message": text}