from fastapi import FastAPI
from pydantic import BaseModel
import os
from datetime import datetime
import dataQuality
import featureMining
import dbTransactions
import shutil
import pandas
from sqlalchemy import create_engine, inspect
import utils

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

    #now we remove the original columns except address, price, description, 
    #living_area_m2, plot_area_m2 and volume_m3
    keep = ["address", "price", "description", "living_area_m2", "plot_area_m2", "volume_m3"]
    columnNames = [x for x in list(pandas.read_csv(datapath)) if x not in keep]

    clean = mined.drop(columnNames, axis=1)

    #writing clean data to bucket
    if os.path.exists("data/clean")==False:
        os.mkdir("data/clean")
    datapath = os.path.join(os.getcwd(), "data/clean/clean_data_" + str(datetime.now()) + ".csv")
    clean.to_csv(datapath, index=False)

    #preparing DB folder
    if os.path.exists("data/db")==False:
        os.mkdir("data/db")
    dbpath = os.path.join(os.getcwd(), "data/db/db_log_" + str(datetime.now()) + ".txt")

    #now is the time to send all this data to the database
    #first we specify the connection
    engine = create_engine(os.environ["SQL_ACCESSKEY"])
    inspector = inspect(engine)
    last_table = inspector.get_table_names()[-1]
    if '-' in last_table:
        new_table = "openred_clean_0"
    else:
        new_table = "openred_clean_" + str(int(last_table.split("_")[-1]) + 1)

    with engine.connect() as conn:

        table = dbTransactions.generate_sql_table(clean, new_table, engine)

        dbTransactions.bulk_insert(clean, table, conn)

        conn.close()

        with open(dbpath, "w") as report:
            report.write("Table {} created on {}.".format(new_table, datetime.now()))
    
    #finally we send a final webhook to make sure the pipeline is finished
    utils.sendWebhook(dbpath)

@app.post("/insert")
async def insert_pipeline(args: Request):

    #writing raw data to bucket
    body = args.body
    if os.path.exists("data/raw/new_entries")==False:
        os.mkdir("data/raw/new_entries")
    datapath = os.path.join(os.getcwd(), "data/raw/new_entries/raw_data_" + str(datetime.now()) + ".csv")
    dqpath = os.path.join(os.getcwd(), "data/data_quality/dq_log_" + str(datetime.now()) + ".txt")
    with open(datapath, "w") as fp:
        fp.write(body)

    #we first check if the schema is compatible by comparing the columns in the
    #new entry with the columns in the master

    last_file = os.listdir("data/raw")[-1]
    columnSchema = list(pandas.read_csv(os.path.join("data/raw", last_file)))

    if list(pandas.read_csv(datapath)) == columnSchema:

        #running data quality
        complete = dataQuality.runDataQuality(datapath, dqpath)

        fmpath = os.path.join(os.getcwd(), "data/feature_mining/fm_log_" + str(datetime.now()) + ".txt")

        #running feature mining pipeline
        mined = featureMining.runFeatureMining(complete, fmpath)

        #now we remove the original columns except address, price, description, 
        #living_area_m2, plot_area_m2 and volume_m3
        keep = ["address", "price", "description", "living_area_m2", "plot_area_m2", "volume_m3"]
        columnNames = [x for x in list(pandas.read_csv(datapath)) if x not in keep]

        clean = mined.drop(columnNames, axis=1)

        dbpath = os.path.join(os.getcwd(), "data/db/db_log_" + str(datetime.now()) + ".txt")

        #now is the time to send all this data to the database
        #first we specify the connection
        engine = create_engine(os.environ["SQL_ACCESSKEY"])
        inspector = inspect(engine)
        last_table = inspector.get_table_names()[-1]

        with engine.connect() as conn:

            dbTransactions.bulk_insert(clean, last_table, conn)

            conn.close()

            with open(dbpath, "w") as report:
                report.write("Insert made on {} on {}.".format(last_table, datetime.now()))

        #finally we send a final webhook to make sure the pipeline is finished
        utils.sendWebhook(dbpath)

    else:

        with open(dqpath, "w") as report:
            report.write("Mismatching data sent on {}.".format(datetime.now()))

        utils.sendWebhook(dqpath)

        raise KeyError("The column names do not match the schema!")



@app.get("/check")
async def check_result():
    with open(os.path.join(os.getcwd(), "data/test.txt"), "r") as fp:
        text = fp.read()
        return {"message": text}