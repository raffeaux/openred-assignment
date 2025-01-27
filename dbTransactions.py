from sqlalchemy import create_engine
from sqlalchemy import Table, Column,  MetaData, Integer, String, Float, Boolean, insert
import pandas
import numpy

def generate_sql_table(df, name, engine):
    """
    Generates a SQL table out of a pandas DataFrame.
    """

    metadata = MetaData()

    type_dict = {'object': String(), 
                 'float64': Float(),
                 'int64': Integer()}
    
    type_list = [str(x) for x in df.dtypes]
    column_types = [type_dict[x] for x in type_list]

    #creating the sensor data table with strings to account for missing values
    #but saving the type for each column

    string_dict = {c : Column(c, t) for (c, t) in zip(list(df), column_types)}

    struct = Table(name, metadata, *list(string_dict.values()))

    metadata.create_all(engine)
    
    return struct

def bulk_insert(df, table, connection):
    """
    Inserts a pandas DataFrame to an existing SQL table.
    """
    
    dummy_records = df.to_dict("records")
    query = insert(table)
    Result = connection.execute(query, dummy_records)
    connection.commit()