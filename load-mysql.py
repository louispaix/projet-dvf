import sys
import pandas as pd
import numpy as np
import mysql.connector


def main() -> int:
    host = "localhost"
    user = "root"
    password = "root"

    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
        )
        if connection.is_connected():
            print(
                f"Successfully connected to MySQL server (version {connection.get_server_info()})"
            )

    except mysql.connector.Error as e:
        print(f"Connection to server failed: {e}")
        return 1

    else:
        cursor = connection.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS dvf")
        cursor.execute("USE dvf")
        cursor.execute("DROP TABLE IF EXISTS data")
        create_table_query = """
            CREATE TABLE data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                no_disposition INT,
                date_mutation DATE,
                nature_mutation VARCHAR(100),
                valeur_fonciere DECIMAL,
                no_voie DECIMAL,
                btq VARCHAR(30),
                type_voie VARCHAR(30),
                code_voie VARCHAR(30),
                voie VARCHAR(30),
                code_postal VARCHAR(30),
                commune VARCHAR(30),
                code_departement VARCHAR(30),
                code_commune VARCHAR(30),
                prefixe_section VARCHAR(30),
                section VARCHAR(30),
                no_plan INT,
                no_volume VARCHAR(30),
                lot_1 VARCHAR(30),
                carrez_lot_1 DECIMAL,
                lot_2 VARCHAR(30),
                carrez_lot_2 DECIMAL,
                lot_3 VARCHAR(30),
                carrez_lot_3 DECIMAL,
                lot_4 VARCHAR(30),
                carrez_lot_4 DECIMAL,
                lot_5 VARCHAR(30),
                carrez_lot_5 DECIMAL,
                nombre_lots INT,
                code_type_local DECIMAL,
                type_local VARCHAR(100),
                surface_reelle_bati DECIMAL,
                nombre_pieces_principales DECIMAL,
                nature_culture VARCHAR(30),
                nature_culture_speciale VARCHAR(30),
                surface_terrain DECIMAL
            )
        """
        cursor.execute(create_table_query)

        filename = "data/data.txt"
        df = pd.read_csv(filename, sep="|", low_memory=False)
        df["Date mutation"] = pd.to_datetime(df["Date mutation"], format="%d/%m/%Y")
        columns = df.columns[df.isna().sum() != df.shape[0]].to_list()

        cursor.execute("DESCRIBE data")
        table_columns = [cols[0] for cols in cursor.fetchall()][1:]

        print("Importing data...")
        for _, record in df[columns].iterrows():
            insert_query = f"""
                INSERT INTO data (
                    {", ".join(table_columns)}
                ) VALUES (
                    {", ".join(["%s"] * len(columns))}
                )
            """
            record.replace(np.nan, None, inplace=True)
            cursor.execute(insert_query, tuple(record.values))

        connection.commit()
        print("Done.")

        connection.close()
        print("Connection closed.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
