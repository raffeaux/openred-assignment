import re

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
                  "address":[True if type(x)==str else False for x in data["address"]],
                  "description":[True if type(x)==str else False for x in data["description"]],
                  "number_of_rooms":[re.search("kamer|slaapkamer", x) if type(x)==str else False for x in data["number_of_rooms"]],
                  "number_of_floors":[re.search("woonlaag|woonlage", x) if type(x)==str else False for x in data["number_of_floors"]],
                  "backyard":[re.search("m²|diep", x) if type(x)==str else False for x in data["backyard"]],
                  "floor_level":[re.search("woonlaag|Begane grond", x) if type(x)==str else False for x in data["floor_level"]],
                  "number_of_bathrooms":[re.search("badkamer|toilet", x) if type(x)==str else False for x in data["number_of_bathrooms"]],
                  "construction_type":[re.search("bouw", x) if type(x)==str else False for x in data["construction_type"]],
                  "housing_type":[re.search("appartement|woning|flat|Maisonnette|pand", x) if type(x)==str else False for x in data["housing_type"]],
                  "energy_label":[re.search("A|B|C|D|E|F|G", x) if type(x)==str else False for x in data["energy_label"]],
                  "housing_status":[re.search("Beschikbaar|Verkocht onder voorbehoud|Onder bod|Onder optie", x) if type(x)==str else False for x in data["housing_status"]],
                  "garage":[re.search("garage|Garage|carport|Carport|inpandig|Inpandig|Parkeer|parkeer|mogelijk", x) if type(x)==str else False for x in data["garage"]],
                  "garden":[re.search("tuin|terras|Patio|Plaats", x) if type(x)==str else False for x in data["garden"]],
                  "ownership":[re.search("erfpacht|Erfpacht|eigendom|Eigendom|belast|Belast|mandelig|Mandelig|Lidmaatschapsrecht|bewoning", x) if type(x)==str else False for x in data["ownership"]]
                 }
    
    return conditions