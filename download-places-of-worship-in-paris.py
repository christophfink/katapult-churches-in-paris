#!/usr/bin/env python3

"""Download all places of worship within Paris, and the greater Paris area."""

import json
import pathlib

import geopandas
import pandas
import requests
import shapely.geometry


OVERPASS_API_URL = "https://lz4.overpass-api.de/api/interpreter"

DATA_DIRECTORY = pathlib.Path().resolve()
OUTPUT_FILE = DATA_DIRECTORY / "katapult.gpkg"
VILLES_DANS_LA_UNITÉ_URBAINE_SPREADSHEET = (
    DATA_DIRECTORY / "Unité_urbaine_de_Paris.ods"
)

VILLES_DANS_LA_UNITÉ_URBAINE = pandas.read_excel(
    VILLES_DANS_LA_UNITÉ_URBAINE_SPREADSHEET
)["CodeInsee"].to_list()

# Overpass Such-Queries
PARIS = "area[\"ISO3166-2\"=\"FR-75\"];"
UNITÉ_URBAINE = "".join(
    [
        f'area["ref:INSEE"="{ville}"];'
        for ville in VILLES_DANS_LA_UNITÉ_URBAINE
    ]
)

# Die Unité Urbaine de Paris besteht aus Paris (75), den gesamten Departements
# Haute-de-Seine (92), Seine-Saint-Denis (93), und Val-de-Marne (94), und
# einer Reihe einzelner Gemeinden in anderen Departements (im Query oben mit
# ihrer jeweiligen INSEE-Nummer referenziert)
# Siehe, u.a., https://fr.wikipedia.org/wiki/Unit%C3%A9_urbaine_de_Paris


def overpass_element_to_point(element):
    try:
        geometry = shapely.geometry.Point(element["lon"], element["lat"])
    except KeyError:
        try:
            geometry = shapely.geometry.Point(
                element["center"]["lon"],
                element["center"]["lat"]
            )
        except KeyError:
            print(element)
            geometry = None
    return geometry


def overpass_json_points_to_geopandas(overpass_json):
    """Convert a response from the Overpass API into a GeoDataFrame."""
    columns = set([
        tag
        for element in overpass_json["elements"]
        for tag in element["tags"]
    ])
    gdf = geopandas.GeoDataFrame(
        {
            column: [
                element["tags"][column] if column in element["tags"] else None
                for element in overpass_json["elements"]
            ]
            for column in columns
        }
        | {
            "geometry": [
                overpass_element_to_point(element)
                for element in overpass_json["elements"]
            ]
        },
        crs="EPSG:4326"
    )
    return gdf


def download_places_of_worship():
    for name, area in (
        ("Paris", PARIS),
        ("UniteUrbaine", UNITÉ_URBAINE)
    ):
        overpass_query = (
            "[out:json][timeout:600];"
            f"({area})->.a;"
            "(node[\"amenity\"=\"place_of_worship\"](area.a);"
            " way[\"amenity\"=\"place_of_worship\"](area.a);"
            " rel[\"amenity\"=\"place_of_worship\"](area.a);"
            ");"
            "out center; "
        )
        with requests.post(  # die Payload ist zu groß für GET
            OVERPASS_API_URL,
            data={"data": overpass_query}
        ) as response:
            try:
                data = response.json()

                # Cache-Kopie für den Fall der Fälle
                with open(f"{name}.json", "w") as f:
                    f.write(json.dumps(data))

                data = overpass_json_points_to_geopandas(data)

                # eliminate duplicate features
                data["wkt"] = data.geometry.wkt
                data = data.groupby("wkt").first()
                data = data.reset_index(drop=True)

                data.to_file(
                    OUTPUT_FILE,
                    layer=name
                )

            except Exception as exception:
                print(exception, response.text)
                raise RuntimeError from exception


def main():
    download_places_of_worship()


if __name__ == "__main__":
    main()
