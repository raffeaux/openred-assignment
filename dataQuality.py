import pandas
from datetime import datetime
import logic
import numpy
import utils

def uniqueness(datapath, dqpath):
    """
    Checks raw data for duplicated rows and keys and immediately
    drops them if they exist. It also logs the results to
    a text file.

    Input:

    datapath: a path-like object -> str
    dqpath: a path-like object -> str

    Output:

    unique: data without duplicates -> pandas.DataFrame
    """

    #reading the raw data
    df = pandas.read_csv(datapath)

    #collecting timestap for current run
    current_time = str(datetime.now())

    #we first check for entire rows that are repeated (double entry etc.)
    df["duplicate"] = df.duplicated(keep=False)
    count_duplicates = df["duplicate"].sum()
    with open(dqpath, "w") as report:
        report.write("Data quality report for the run at {}.\n\nThere were {} duplicate rows. They have been dropped\n".format(current_time, count_duplicates)) 

    #the main ID is address, so as a second step we check that it is unique too
    df["duplicate_address"] = df.duplicated("address", keep=False)
    duplicate_address = df["duplicate_address"].sum()
    with open(dqpath, "a") as report:
        report.write("There were {} duplicate keys. These have been dropped.\n".format(duplicate_address))

    unique = df[(df["duplicate"]==False) & (df["duplicate_address"]==False)].drop(["duplicate", "duplicate_address"], axis=1).reset_index(drop=True)

    return unique

def validity(data, dqpath):
    """
    Checks unique data rows for invalid entries. These entries are
    flagged and the invalid field is marked for analytics purposes.
    Then the invalid value is replaced by NaN so that it may be 
    treated as missing in following steps. The results are logged
    to a text file.

    Input:

    data: a dataset previously checked for uniqueness -> pandas.DataFrame
    dqpath: a path-like object -> str

    Output:

    valid: data without invalid entries -> pandas.DataFrame
    """

    #we set conditions for validity of each column
    conditions = logic.conditionsValidity(data)

    #we create two flag columns to identify invalid entries
    #and which field is invalid
    data["FLAG_WAS_INVALID"] = 0
    data["FLAG_INVALID_FIELD"] = ""

    #we use the conditions to fill in the flags
    for column in list(conditions.keys()):

        data.loc[(conditions[column]==False)&(data[column].isna()==False), "FLAG_WAS_INVALID"] = 1
        data.loc[(conditions[column]==False)&(data[column].isna()==False), "FLAG_INVALID_FIELD"] = column
        data.loc[(conditions[column]==False)&(data[column].isna()==False), column] = numpy.nan

    #we calculate the number of invalid observations and 
    #isolate invalid fields
    totalInvalid = data["FLAG_WAS_INVALID"].sum()
    invalidFields = data["FLAG_INVALID_FIELD"][data["FLAG_WAS_INVALID"]==1].unique()

    #we write all this info down
    with open(dqpath, "a") as report:
        report.write("There were {} rows with invalid entries.\nThe invalid fields are: {}.\n".format(totalInvalid, invalidFields))

    valid = data.copy()

    return valid


def completeness(data, dqpath):
    """
    Checks unique and valid data rows for missing values, dropping rows
    where these render the row useless or otherwise using flags to 
    explain the missing values (e.g. if they are not really missing
    but just not relevant for that row). The results are written to log.

    Input:

    data: a dataset previously checked for uniqueness and validity -> pandas.DataFrame
    dqpath: a path-like object -> str

    Output:

    flagged: data with explained missing values -> pandas.DataFrame
    """

    #we set conditions for completeness of each column
    conditions = logic.conditionsCompleteness(data)

    #we use the conditions to fill in the flags
    for column in list(conditions.keys()):
        data[conditions[column][1]] = 0
        data.loc[(data[column].isna())&(conditions[column][0]), conditions[column][1]] = 1
    
    #we calculate the total number of missing values and those explained by flags
    totalMissing = sum([data[column].isna().sum() for column in list(conditions.keys())])
    totalExplained = sum([data[conditions[column][1]][data[column].isna()].sum() for column in list(conditions.keys())])

    #we write all this info down
    with open(dqpath, "a") as report:
        report.write("There were {} missing values.\nOf these, {} could be explained by flags.\n".format(totalMissing, totalExplained))

    flagged = data[data["price"].isna()==False].reset_index(drop=True)

    return flagged

def runDataQuality(datapath, dqpath):
    """
    Wrapper function for the data quality pipeline. It consists of
    three steps:
    
    1) Uniqueness: checks for duplicate entries and drops them
    2) Validity: checks for invalid entries and replaces them with NaN
    3) Completeness: checks for missing values and flags them if possible

    These steps are used to fill in a log which can be found in the
    data quality folder and are also sent via webhook.

    Input:

    datapath: a path-like object -> str
    dqpath: a path-like object -> str

    Output:

    complete: a pandas.DataFrame object -> pandas.DataFrame
    """

    unique = uniqueness(datapath, dqpath)
    valid = validity(unique, dqpath)
    complete = completeness(valid, dqpath)

    utils.sendWebhook(dqpath)

    return complete



