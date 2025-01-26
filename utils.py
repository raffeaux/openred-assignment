import re
import pandas

def set_conditions(data):
    """
    Sets conditions and boundaries for each variable in the
    dataset to ensure its validity. These conditions are 
    based on previous data exploration and common sense.

    Input:

    data: a dataset with unique rows -> pandas.DataFrame

    Output:

    conditions: a dict-like object -> dict
    """

    conditions = {
                    "price":data["price"]>50000,
                    "living_area_m2":data["living_area_m2"]>20,
                    "plot_area_m2":data["plot_area_m2"]>20,
                    "volume_m3":data["volume_m3"]>50,
                    "address":pandas.Series([True if type(x)==str else False for x in data["address"]]),
                    "description":pandas.Series([True if type(x)==str else False for x in data["description"]]),
                    "number_of_rooms":pandas.Series([bool(re.search("kamer|slaapkamer", x)) if type(x)==str else False for x in data["number_of_rooms"]]),
                    "number_of_floors":pandas.Series([bool(re.search("woonlaag|woonlage", x)) if type(x)==str else False for x in data["number_of_floors"]]),
                    "backyard":pandas.Series([bool(re.search("m²|diep", x)) if type(x)==str else False for x in data["backyard"]]),
                    "floor_level":pandas.Series([bool(re.search("woonlaag|Begane grond", x)) if type(x)==str else False for x in data["floor_level"]]),
                    "number_of_bathrooms":pandas.Series([bool(re.search("badkamer|toilet", x)) if type(x)==str else False for x in data["number_of_bathrooms"]]),
                    "construction_type":pandas.Series([bool(re.search("bouw", x)) if type(x)==str else False for x in data["construction_type"]]),
                    "housing_type":pandas.Series([bool(re.search("appartement|woning|flat|Maisonnette|pand", x)) if type(x)==str else False for x in data["housing_type"]]),
                    "energy_label":pandas.Series([bool(re.search("A|B|C|D|E|F|G", x)) if type(x)==str else False for x in data["energy_label"]]),
                    "housing_status":pandas.Series([bool(re.search("Beschikbaar|Verkocht onder voorbehoud|Onder bod|Onder optie", x)) if type(x)==str else False for x in data["housing_status"]]),
                    "garage":pandas.Series([bool(re.search("garage|Garage|carport|Carport|inpandig|Inpandig|Parkeer|parkeer|mogelijk", x)) if type(x)==str else False for x in data["garage"]]),
                    "garden":pandas.Series([bool(re.search("tuin|terras|Patio|Plaats", x)) if type(x)==str else False for x in data["garden"]]),
                    "ownership":pandas.Series([bool(re.search("erfpacht|Erfpacht|eigendom|Eigendom|belast|Belast|mandelig|Mandelig|Lidmaatschapsrecht|bewoning", x)) if type(x)==str else False for x in data["ownership"]])
                 }
    
    return conditions