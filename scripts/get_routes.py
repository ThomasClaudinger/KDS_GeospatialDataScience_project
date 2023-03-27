import requests
import json
from shapely.geometry import shape
import geopandas as gpd
import pandas as pd
import time


def main():
    counter_start = 14003

    url = "https://api.openrouteservice.org/v2/directions/cycling-regular/geojson"
    body = {"coordinates":[[10.781088,59.908227],[10.7544337247013,59.91218291295141]],"attributes":["percentage"],"extra_info":["surface","waycategory","waytype"],"preference":"recommended","roundabout_exits":"true","geometry":"true"}
    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Authorization': '5b3ce3597851110001cf62481780b7770b914a96a64e38112581d6ee',
        'Content-Type': 'application/json; charset=utf-8'
    }

    bike_data = pd.read_csv("data/july_2022_filtered_renamed.csv")
    bike_data = bike_data.drop(columns=["Unnamed: 0"])
    bike_data_subset = bike_data.iloc[counter_start:]

    route_data_list = []
    geometry_list = []

    counter = 0
    for _, row in bike_data_subset.iterrows():
        start_time = time.time()
        route_data = row.copy()
        start_lat = row["st_st_lat"]
        start_lon = row["st_st_lon"]
        end_lat = row["en_st_lat"]
        end_lon = row["en_st_lon"]

        body["coordinates"] = [[start_lon,start_lat],[end_lon,end_lat]]

        try:
            r=requests.post(url, json=body, headers=headers)
        except Exception:
            print("API EXCEPTION")
            time.sleep(1.1)
            counter += 1
            continue

        try:
            response = json.loads(r.text)

            properties = response["features"][0]["properties"]
            geometry = response["features"][0]["geometry"]
            geom = shape(geometry)
            distance = properties["summary"]["distance"]
            waytypes = properties["extras"]["waytypes"]

            route_data["total_dist"] = distance

            waytypes_data = ""
            waytypes_summary = ""

            for data_row in waytypes["values"]:
                waytypes_data += " ".join(str(x) for x in data_row) + ";"

            for summary_row in waytypes["summary"]:
                waytypes_summary += " ".join(str(k)[0]+":"+str(v) for k, v in summary_row.items()) + ";"
                # value - which waytype, distance - in m, amount - in %

            route_data["waytypes_d"] = waytypes_data # waytypes data
            route_data["waytypes_s"] = waytypes_summary # waytypes summary

            route_data_list.append(route_data.to_dict())
            geometry_list.append(geom)
            time.sleep(1.6)
            counter += 1
            end_time = time.time()
            print(f"{counter} ({r.status_code}), fetch time: {end_time-start_time} [s]")
            if counter >= 4000:
                break

        except Exception:
            print("UNEXPECTED EXCEPTION")
            counter += 1
            continue
    
    counter_end = counter_start + counter
    gdf = gpd.GeoDataFrame(route_data_list, geometry=geometry_list, crs="EPSG:4326")
    gdf.to_file(f"data/partial/data_last_fetched_index_{counter_end}.shp")


if __name__ == "__main__":
    main()
