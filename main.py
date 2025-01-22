from fastapi import FastAPI
from pydantic import BaseModel
import os

app = FastAPI()

class Request(BaseModel):
    body: str

@app.get("/")
async def root():
    return {"message": os.listdir(os.getcwd())}

@app.post("/start")
async def start_pipeline(body: Request):
    text = body.body
    with open(os.path.join(os.getcwd(), "raw/test.txt"), "w") as fp:
        fp.write(text)

@app.get("/check")
async def check_result():
    with open(os.path.join(os.getcwd(), "raw/test.txt"), "r") as fp:
        text = fp.read()
        return {"message": text}