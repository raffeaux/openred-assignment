import pandas
import numpy
import utils
import logic
from datetime import datetime

def extractFeaturesNumerics(df, fmpath):
    """
    Follows rules and conditions to extract usable features
    from the numerical columns of the scraped data. The
    categorical columns are approached in the function below,
    extractFeaturesCategorical. The results are logged in
    a text file.

    Input:

    df: a dataset which has already passed Data Quality -> pandas.DataFrame
    fmpath: a path-like object -> str

    Output: 

    numerics: a dataset with numerical features mined -> pandas.DataFrame

    """

    #getting current time
    current_time = datetime.now()

    #defining conditions to approach feature extraction
    conditions = logic.conditionsFeatureMining(df)

    #we use the conditions to generate the new columns
    for column in list(conditions.keys()):
        df[conditions[column][1]] = conditions[column][0]

    #we write a log
    with open(fmpath, "w") as report:
        report.write("Feature Engineering for the run on {}.\n\nThe numerical part finished without errors.\n".format(current_time))

    numerics = df.copy()

    return numerics


def extractFeaturesCategorical(df, fmpath):
    """
    Applies binary dummies to all categorical data. This allows
    us to use each dummy as a variable in a model. The results
    are logged in a text file.

    Input:

    df: a dataset which has already passed Data Quality -> pandas.DataFrame
    fmpath: a path-like object -> str

    Output: 

    categoricals: a dataset with categorical features mined -> pandas.DataFrame
    """

    #grouping housing types to reduce dimensions
    df["new_housing_type"] = [x.split(" ")[0].replace(",", "") for x in df["housing_type"]]
    df.loc[df["new_housing_type"]=="Beneden", "new_housing_type"] = "Benedenwoning"
    df.loc[df["new_housing_type"]=="Dubbel", "new_housing_type"] = "Dubbel benedenhuis"

    #we can easily use pandas to create dummies for all columns
    #that have a single value
    categories = ["new_housing_type", "city", "housing_status", "construction_type", "energy_label"]

    for c in categories:
        dummies = pandas.get_dummies(df[c], prefix=c, dtype=int)
        df = pandas.concat([df, dummies], axis=1)

    #there are other columns that contain multiple values; we 
    #have to approach them differently
    multiValues = ["garden", "garage"]

    for column in multiValues:
        colNames = utils.separateDummies(column)
        for name in colNames:
            df[name] = [1 if (type(x)==str)&(name in x) else 0 for x in df[column]]

    #as far as ownership is concerned, because it's just one column
    #perhaps it's not worth the pain to develop a whole logic
    df["volle_eigendom"] = [1 if "olle eigendom" in x else 0 for x in df["ownership"].fillna("no info")]
    df["gemeentelijke_erfpacht"] = [1 if "emeentelijke erfpacht" in x else 0 for x in df["ownership"].fillna("no info")]
    df["gemeentelijke_eigendom"] = [1 if "emeentelijke eigendom" in x else 0 for x in df["ownership"].fillna("no info")]
    df["gebruik_en_bewoning"] = [1 if "ebruik en bewoning" in x else 0 for x in df["ownership"].fillna("no info")]
    df["particulier_eigendom_belast"] = [1 if "articulier eigendom belast" in x else 0 for x in df["ownership"].fillna("no info")]
    df["belast_met_opstal"] = [1 if "elast met opstal" in x else 0 for x in df["ownership"].fillna("no info")]
    df["mandelig"] = [1 if "andelig" in x else 0 for x in df["ownership"].fillna("no info")]
    df["lidmaatschapsrecht"] = [1 if "idmaatschapsrecht" in x else 0 for x in df["ownership"].fillna("no info")]

    #we write a log
    with open(fmpath, "a") as report:
        report.write("The numerical part finished without errors.\n")

    categorics = df.copy()

    return categorics

def runFeatureMining(df, fmpath):
    """
    Wrapper function for the feature mining pipeline. It consists of
    three steps:
    
    1) Uniqueness: checks for duplicate entries and drops them
    2) Validity: checks for invalid entries and replaces them with NaN

    These steps are used to fill in a log which can be found in the
    feature mining folder and are also sent via webhook.

    Input:

    datapath: a path-like object -> str
    dqpath: a path-like object -> str

    Output:

    complete: a pandas.DataFrame object -> pandas.DataFrame
    """

    numerics = extractFeaturesNumerics(df, fmpath)
    categorics = extractFeaturesCategorical(numerics, fmpath)

    utils.sendWebhook(fmpath)

    return categorics
    

    