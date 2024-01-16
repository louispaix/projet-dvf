from pymongo import MongoClient
import pandas as pd
import numpy as np
import sys


database_name = "dvf"
collection_name = "data"


def get_database():
    connection_string = "mongodb+srv://admin:admin@learning-mongo-cluster.wbwdp0z.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(connection_string)
    return client[database_name]


def main() -> int:
    database = get_database()
    print("Successfully connected to MongoDB")
    collection = database[collection_name]
    collection.drop()

    df = pd.read_csv(
        "data/data.txt",
        sep="|",
        low_memory=False,
        dtype={"Code departement": str, "Code postal": str, "Code commune": str},
    )
    df["Date mutation"] = pd.to_datetime(df["Date mutation"], format="%d/%m/%Y")

    desired_types = df["Type local"].isin(["Appartement", "Maison"])
    data = df[desired_types]
    columns = data.columns[data.isna().sum() != data.shape[0]].to_list()

    records = []
    for i, (_, record) in enumerate(data[columns].iloc[:100].iterrows()):
        record.replace(np.nan, None, inplace=True)
        records.append(record.to_dict())
        if (i + 1) % 50_000 == 0:
            collection.insert_many(records)
            records.clear()
            print(f"Inserting data: {i + 1} / {len(data)}")
    collection.insert_many(records)
    print("All data successfully loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
