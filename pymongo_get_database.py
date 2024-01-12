from pymongo import MongoClient
import pandas as pd
import numpy as np


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    connection_string = ("mongodb+srv://louispaix:clusteradmin@projet-big-data.itydq1g.mongodb.net/?retryWrites=true&w"
                         "=majority")

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(connection_string)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['dvf']


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    dbname = get_database()
    collection = dbname["data"]
    collection.drop()

    df = pd.read_csv("data_cleaned.txt", sep="|", low_memory=False, dtype={"Code departement": str, "Code postal": str, "Code commune": str})
    df["Date mutation"] = pd.to_datetime(df["Date mutation"], format="%d/%m/%Y")

    near_paris = df["Code departement"].isin(["75", "77", "78", "91", "92", "93", "94", "95"])
    housing = df["Type local"].isin(["Appartement", "Maison"])
    new_df = df[housing]
    columns = new_df.columns[new_df.isna().sum() != new_df.shape[0]].to_list()

    records = []
    for i, (_, record) in enumerate(new_df[columns].iterrows()):
        record.replace(np.nan, None, inplace=True)
        records.append(record.to_dict())
        if (i+1) % 50_000 == 0:
            collection.insert_many(records)
            records.clear()
            print(i+1)
    collection.insert_many(records)