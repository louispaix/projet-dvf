import os
import pymongo
import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import pandas as pd
import json


filename = "data/cache.csv"
database_name = "dvf"
collection_name = "data"
if os.path.exists(filename):
    print("Using cached data")
    df = pd.read_csv(filename, sep="|", header=0, low_memory=False)
else:
    connection_string = (
        "mongodb+srv://louispaix:clusteradmin@projet-big-data.itydq1g.mongodb.net/?retryWrites=true&w"
        "=majority"
    )
    client = pymongo.MongoClient(connection_string)
    print("Successfully connected to MongoDB")
    db = client[database_name]
    collection = db[collection_name]
    data = collection.find({})
    df = pd.DataFrame(data)
    df.to_csv(filename, sep="|", index=False)
    print(f"Data saved to '{filename}' for later reuse")

df["Prix m2"] = df["Valeur fonciere"] / df["Surface reelle bati"]
price_department = (
    df[["Code departement", "Prix m2"]]
    .groupby(by="Code departement")
    .median()
    .reset_index()
)
price_postal = (
    df[["Code postal", "Prix m2"]].groupby(by="Code postal").median().reset_index()
)
total = (
    df.groupby("Code departement")
    .size()
    .sort_values(ascending=False)
    .to_frame()
    .reset_index()
)
total_df = total.rename(columns={0: "total"})
values_min = total_df.total.min()
values_max = total_df.total.max()

with open("data/department.geojson") as f:
    departments_geo_data = json.load(f)
with open("data/postal.geojson") as f:
    postal_geo_data = json.load(f)

height = 700
app = dash.Dash(__name__)

dep_map = px.choropleth_mapbox(
    price_department,
    geojson=departments_geo_data,
    featureidkey="properties.code",
    locations="Code departement",
    color="Prix m2",
    color_continuous_scale="Viridis",
    range_color=(0, 5000),
    mapbox_style="carto-positron",
    zoom=4,
    center={"lat": 46.6031, "lon": 1.8883},
    opacity=0.5,
    labels={"Prix m2": "Prix au mètre carré"},
    height=height,
)

postal_map = px.choropleth_mapbox(
    price_postal,
    geojson=postal_geo_data,
    featureidkey="properties.codePostal",
    locations="Code postal",
    color="Prix m2",
    color_continuous_scale="Viridis",
    range_color=(0, 5000),
    mapbox_style="carto-positron",
    zoom=4,
    center={"lat": 46.6031, "lon": 1.8883},
    opacity=0.5,
    labels={"Prix m2": "Prix au mètre carré"},
    height=height,
)

dep_total = px.choropleth_mapbox(
    total_df,
    geojson=departments_geo_data,
    featureidkey="properties.code",
    locations="Code departement",
    color="total",
    color_continuous_scale="Viridis",
    range_color=(values_min, values_min),
    mapbox_style="carto-positron",
    zoom=4,
    center={"lat": 46.6031, "lon": 1.8883},
    opacity=0.5,
    labels={"total": "Nombre de demandes"},
    height=height,
)

app.layout = html.Div(
    [
        html.H1("Demande de valeurs foncières", style={"text-align": "center"}),
        html.Div(
            [
                html.Div(
                    [
                        html.Div(id="fig"),
                        dcc.Tabs(
                            id="tabs",
                            value="fig1",
                            children=[
                                dcc.Tab(label="Département", value="tab-dpt"),
                                dcc.Tab(label="Code postal", value="tab-cp"),
                            ],
                            style={"width": "70%", "margin": "auto"},
                        ),
                    ],
                    style={
                        "width": "50%",
                        "display": "inline-block",
                        "verticalAlign": "top",
                    },
                ),
                html.Div(
                    [
                        dcc.Graph(figure=dep_total),
                    ],
                    style={
                        "width": "50%",
                        "display": "inline-block",
                        "verticalAlign": "top",
                    },
                ),
            ]
        ),
    ]
)


@callback(Output("fig", "children"), Input("tabs", "value"))
def render_content(tab):
    if tab == "tab-dpt":
        return html.Div([dcc.Graph(figure=dep_map)])
    else:
        return html.Div([dcc.Graph(figure=postal_map)])


if __name__ == "__main__":
    app.run(debug=True)
