import re
import pandas
import os
import requests
import numpy
import itertools

def sendWebhook(logpath):
    """
    Sends a Slack webhook to report run results, using a 
    previously filled-in text file.

    Input:

    logpath: a path-like object -> str
    """

    with open(logpath, "r") as report:
        url = os.environ["WEBHOOK_URL"]
        payload = {"text":report.read()}
        r = requests.post(url, json=payload)

def separateDummies(values):
    """
    Flattens a categorical variable to reduce its dimensions,
    making the creation of a binary index more feasible. I
    separate by " en " and then by ", ", flattening the
    resulting lists to yield only unique values.

    Input: 
    
    values: a list of values to flatten -> list

    Output:

    unique_values: a list of flattened, unique values -> list

    """

    stage1 = [x.split(" en ") if type(x)==str else numpy.nan for x in values]
    if numpy.nan in stage1:
        stage1.remove(numpy.nan)
    stage2 = [x.split(", ") if type(x)==str else numpy.nan for x in itertools.chain(*stage1)]
    stage3 = [x[1:] for x in set(itertools.chain(*stage2))] #removing the first letter so the expression
    #matches both upper case and lower case
    unique_values = set(stage3)

    return unique_values