import json
import pandas as pd
import plotly.express as px
import pymongo
import streamlit as st



# Connect to database
connection = pymongo.MongoClient(host="localhost", port=27017)
collection = connection["marcellus"]["report.production"]

# Load the county boundaries
with open("/home/miles/Downloads/Pennsylvania County Boundaries.geojson") as fin:
    counties = json.load(fin)

# Load all unique periods
periods = sorted(collection.distinct("production_report.period"))


st.title("Marcellus Gas Well by County")

# Get a dataframe with elements from the first periods
def get_data_by_period(period):
    cursor = collection.aggregate([
        {
            "$match": {
                "production_report.period": period
            }
        },
        {
            "$group": {
                "_id": "$county",
                "sum": {"$sum": 1}
            }
        }
    ])
    records = ({"county": r.get("_id"), "sum": r.get("sum")} for r in cursor)
    df = pd.DataFrame.from_dict(records)
    df["county"] = df["county"].str.upper()
    return df

# Create a sidebar
option_period = st.sidebar.selectbox("Period", periods)


# Make the map
def create_map_from_period(option_period):
    with st.spinner("Getting data..."):
        df = get_data_by_period(option_period)
        df = df.groupby("county")["sum"].sum().reset_index()
        fig = px.choropleth(
            df,
            geojson=counties,
            locations="county",
            featureidkey="properties.county_nam",
            color="sum",
            color_continuous_scale="Viridis",
            scope="usa"
        )
        fig.update_geos(fitbounds="locations")
        fig.update_layout(
            margin=dict(r=0, l=0, t=0, b=0)
        )
    return fig

def create_barchart_from_period(option_period):
    with st.spinner("Getting data..."):
        df = get_data_by_period(option_period)
        df = df.groupby("county")["sum"].sum().reset_index()
        df.sort_values(by="sum", inplace=True)
    return px.bar(df, x="county", y="sum", color_continuous_scale="Viridis")


fig_map = create_map_from_period(option_period)
st.plotly_chart(fig_map)

fig_bar = create_barchart_from_period(option_period)
st.plotly_chart(fig_bar)
