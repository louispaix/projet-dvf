import os
import pymongo
import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import json

connection_string = ("mongodb+srv://louispaix:clusteradmin@projet-big-data.itydq1g.mongodb.net/?retryWrites=true&w"
                     "=majority")

if os.path.exists("data_mongodb.csv"):
    df = pd.read_csv("data_mongodb.csv", sep="|", header=0, low_memory=False)
    print("données déjà récupérées")
else:
    client = pymongo.MongoClient(connection_string)
    db = client["dvf"]
    collection = db["data"]
    data = collection.find({})
    df = pd.DataFrame(data)
    df.to_csv("data_mongodb.csv", sep="|", index=False)
    print("données récupérées depuis mongodb stockées dans data_mongodb.csv")

app = dash.Dash(__name__)
app.layout = html.Div([
    dcc.Graph(id='mongo-plot'),
])

@app.callback(
    dash.dependencies.Output('mongo-plot', 'figure'),
    [dash.dependencies.Input('your-dropdown-or-input', 'value')]  # Add inputs if needed
)
def update_figure(selected_value):
    # Modify the figure based on the selected input or any other logic
    # You can use the selected_value to filter data if you have dropdowns or other inputs
    updated_fig = px.scatter(df, x='your_x_column', y='your_y_column', title='Updated MongoDB Data Visualization')
    return updated_fig


df["Prix m2"] = df["Valeur fonciere"] / df["Surface reelle bati"]
prix_departements = df[["Code departement", "Prix m2"]].groupby(by="Code departement").median().reset_index()
prix_code_postaux= df[["Code postal", "Prix m2"]].groupby(by="Code postal").median().reset_index()

with open("departements.geojson") as f:
    departements = json.load(f)
with open("contours-codes-postaux.geojson") as f:
    codes_postaux = json.load(f)

# Plot the choropleth map
fig = px.choropleth_mapbox(
    prix_departements,
    geojson=departements,
    featureidkey='properties.code',
    locations='Code departement', color='Prix m2',
    color_continuous_scale="Viridis",
    range_color=(0, 5000),  # Adjusted range_color
    mapbox_style="carto-positron",
    zoom=5.4, center={"lat": 46.6031, "lon": 1.8883},
    opacity=0.5,
    labels={'Prix m2': 'Prix au mètre carré'}
)
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
fig.show()

# Plot the choropleth map
fig = px.choropleth_mapbox(
    prix_code_postaux,
    geojson=codes_postaux,
    featureidkey='properties.codePostal',
    locations='Code postal', color='Prix m2',
    color_continuous_scale="Viridis",
    range_color=(0, 5000),  # Adjusted range_color
    mapbox_style="carto-positron",
    zoom=5.4, center={"lat": 46.6031, "lon": 1.8883},
    opacity=0.5,
    labels={'Prix m2': 'Prix au mètre carré'}
)
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
fig.show()