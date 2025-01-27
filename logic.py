import re
import pandas
import numpy

def conditionsValidity(data):
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
                  "housing_type":pandas.Series([bool(re.search("appartement|woning|flat|Maisonnette|pand|verdieping|Penthouse", x)) if type(x)==str else False for x in data["housing_type"]]),
                  "energy_label":pandas.Series([bool(re.search("A|B|C|D|E|F|G|Niet verplicht|Niet beschikbaar", x)) if type(x)==str else False for x in data["energy_label"]]),
                  "housing_status":pandas.Series([bool(re.search("Beschikbaar|Verkocht onder voorbehoud|Onder bod|Onder optie", x)) if type(x)==str else False for x in data["housing_status"]]),
                  "garage":pandas.Series([bool(re.search("garage|Garage|carport|Carport|inpandig|Inpandig|Parkeer|parkeer|mogelijk|Souterrain", x)) if type(x)==str else False for x in data["garage"]]),
                  "garden":pandas.Series([bool(re.search("tuin|Tuin|terras|Patio|Plaats", x)) if type(x)==str else False for x in data["garden"]]),
                  "ownership":pandas.Series([bool(re.search("erfpacht|Erfpacht|eigendom|Eigendom|belast|Belast|mandelig|Mandelig|Lidmaatschapsrecht|bewoning", x)) if type(x)==str else False for x in data["ownership"]])
                }
    
    return conditions

def conditionsCompleteness(data):
    """
    Sets conditions to establish whether an entry is truly
    missing, not relevant to that specific row, or perhaps there
    has been an issue while scraping. These conditions are based
    on previous data exploration and common sense.

    Input:

    data: a dataset with unique and valid rows -> pandas.DataFrame

    Output:

    conditions: a dict-like object -> dict
    """

    conditions = {
                  "price":[pandas.Series([bool(re.search("prijs|Prijs|euro|€", x)) if type(x)==str else False for x in data["description"]]), "FLAG_PRICE_IN_DESCRIPTION"],
                  "living_area_m2":[pandas.Series([bool(re.search("woonoppervlakte|Woonoppervlakte|gebruiksoppervlakte|Gebruiksoppervlakte", x)) if type(x)==str else False for x in data["description"]]), "FLAG_LIVING_AREA_IN_DESCRIPTION"],
                  "plot_area_m2":[pandas.Series([bool(re.search("appartement|flat", x)) if type(x)==str else False for x in data["housing_type"]]), "FLAG_NO_PLOT_IS_APARTMENT"],
                  "volume_m3":[pandas.Series([bool(re.search("volume|Volume", x)) if type(x)==str else False for x in data["description"]]), "FLAG_VOLUME_IN_DESCRIPTION"],
                  "address":[pandas.Series([bool(re.search("straat|weg|kade|kwartier|adres|Adres", x)) if type(x)==str else False for x in data["description"]]), "FLAG_ADDRESS_MAYBE_IN_DESCRIPTION"],
                  "description":[pandas.Series([False if type(x)==str else True for x in data["description"]]), "FLAG_DESCRIPTION_MISSING"],
                  "number_of_rooms":[pandas.Series([bool(re.search("kamer|Kamer|slaapkamer|Slaapkamer", x)) if type(x)==str else False for x in data["description"]]), "FLAG_ROOMS_IN_DESCRIPTION"],
                  "number_of_floors":[pandas.Series([bool(re.search("woonlaag|woonlage", x)) if type(x)==str else False for x in data["description"]]), "FLAG_FLOORS_IN_DESCRIPTION"],
                  "backyard":[pandas.Series([bool(re.search("Begane grond", x))==False if type(x)==str else False for x in data["description"]]), "FLAG_ABOVE_GROUND_NO_YARD"],
                  "floor_level":[pandas.Series([bool(re.search("appartement|flat|Penthouse|Maisonnette|Tussenverdieping", x))==False if type(x)==str else False for x in data["housing_type"]]), "FLAG_SINGLE_HOUSE_NO_SPECIFIC_FLOOR"],
                  "number_of_bathrooms":[pandas.Series([bool(re.search("badkamer|Badkamer|toilet|Toilet", x)) if type(x)==str else False for x in data["description"]]), "FLAG_BATHROOM_IN_DESCRIPTION"],
                  "construction_type":[pandas.Series([bool(re.search("Nieuwbouw|Bestaande bouw", x)) if type(x)==str else False for x in data["description"]]), "FLAG_CONSTRUCTION_IN_DESCRIPTION"],
                  "housing_type":[pandas.Series([bool(re.search("appartement|Appartement|woning|flat|Maisonnette|maisonnette|Grachtenpand|grachtenpand|Tussenverdieping|tussenverdieping|Penthouse|penthouse|Herenhuis|herenhuis|Woonboerderij|woonboerderij", x)) if type(x)==str else False for x in data["description"]]), "FLAG_TYPE_IN_DESCRIPTION"],
                  "energy_label":[pandas.Series([bool(re.search("energielabel|Energielabel|energie|Energie", x)) if type(x)==str else False for x in data["description"]]), "FLAG_ENERGY_IN_DESCRIPTION"],
                  "housing_status":[pandas.Series([bool(re.search("Beschikbaar|Verkocht onder voorbehoud|Onder bod|Onder optie", x)) if type(x)==str else False for x in data["description"]]), "FLAG_STATUS_IN_DESCRIPTION"],
                  "garage":[pandas.Series([bool(re.search("garage|Garage|carport|Carport|inpandig|Inpandig|Parkeer|parkeer|Souterrain", x)) if type(x)==str else False for x in data["description"]]), "FLAG_GARAGE_IN_DESCRIPTION"],
                  "garden":[pandas.Series([bool(re.search("tuin|Tuin", x)) if type(x)==str else False for x in data["description"]]), "FLAG_GARDEN_IN_DESCRIPTION"],
                  "ownership":[pandas.Series([bool(re.search("erfpacht|Erfpacht|eigendom|Eigendom|belast|Belast", x)) if type(x)==str else False for x in data["description"]]), "FLAG_OWNERSHIP_IN_DESC"]
                 }
    
    return conditions

def conditionsFeatureMining(data):
    """
    Sets conditions and boundaries for each variable in the
    dataset to determine how to approach feature mining.
    These conditions are based on previous data exploration 
    and common sense.

    Input:

    data: a dataset which has already passed Data Quality -> pandas.DataFrame

    Output:

    conditions: a dict-like object -> dict
    """

    conditions = {
                  "address":[pandas.Series([x.split(" ")[-1] if type(x)==str else numpy.nan for x in data["address"]]), "city"],
                  "number_of_rooms":[pandas.Series([int(x.split(" kamer")[0]) if type(x)==str else numpy.nan for x in data["number_of_rooms"]]), "totalRooms"],
                  "number_of_rooms":[pandas.Series([int(x.split(" slaapkamer")[0].split("(")[1]) if (" slaapkamer" in x)&("(" in x) else numpy.nan for x in data["number_of_rooms"].fillna("no_info")]), "bedrooms"],
                  "number_of_floors":[pandas.Series([int(x.split(" woonla")[0]) if (type(x)==str)&("woonla" in x) else numpy.nan for x in data["number_of_floors"]]), "totalFloors"],
                  "number_of_floors":[pandas.Series([1 if (type(x)==str)&("zolder" in x) else 0 for x in data["number_of_floors"]]), "zolder"],
                  "number_of_floors":[pandas.Series([1 if (type(x)==str)&("kelder" in x) else 0 for x in data["number_of_floors"]]), "kelder"],
                  "number_of_floors":[pandas.Series([1 if (type(x)==str)&("vliering" in x) else 0 for x in data["number_of_floors"]]), "vliering"],
                  "backyard":[pandas.Series([float(x.split(" m²")[0]) if " m²" in x else numpy.nan for x in data["backyard"].fillna("no info")]), "backyardM2"],
                  "floor_level":[pandas.Series([int(x.split("e woonlaag")[0]) if "e woonlaag" in x else 0 for x in data["floor_level"].fillna("no info")]), "whichFloor"],
                  "number_of_bathrooms":[pandas.Series([int(x.split(" badkamer")[0]) if " badkamer" in x else numpy.nan for x in data["number_of_bathrooms"].fillna("no info")]), "bathrooms"],
                  "number_of_bathrooms":[pandas.Series([int(x.split(" apart")[0][-1]) if " apart" in x else numpy.nan for x in data["number_of_bathrooms"].fillna("no_info")]), "separateToilets"]
                 }
    
    return conditions