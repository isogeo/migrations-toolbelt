# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create the list of Rouen service layers association before Service metadata deletion 
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
from os import environ
from pathlib import Path
# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/rouen.env", override=True)

if __name__ == "__main__":

    # ############################### MIGRATING & SAVING ###############################
    # API client instanciation
    isogeo = Isogeo(
        client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
        auth_mode="user_legacy",
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
    )
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )

    workgroup_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    service_md_uuid = ("2e709eeb72884daf97a07ec927111978", "3ca708da46dc4fe8ba23f0a05e36ab15")

    # Search about all workgroup metadatas because there are less than 800
    service_md_search = isogeo.search(
        group=workgroup_uuid,
        specific_md=service_md_uuid,
        include="all"
    )
    isogeo.close()

    li_for_csv = []

    for md in service_md_search.results:
        service_md_uuid = md.get("_id")
        service_md_title = md.get("title")

        for layer in md.get("layers"):
            csv_line = [service_md_title, service_md_uuid]

            isogeo_layer_id = layer.get("_id")
            service_layer_id = layer.get("id")
            layer_titles = ""
            for title in layer.get("titles"):
                layer_titles += "{};".format(title.get("value"))
            layer_titles = layer_titles[:-1]

            csv_line.append(isogeo_layer_id)
            csv_line.append(service_layer_id)
            csv_line.append(layer_titles)

            if layer.get("dataset"):
                dataset = layer.get("dataset")
                csv_line.append(dataset.get("_id"))
                csv_line.append(dataset.get("name"))
                csv_line.append(dataset.get("title"))
            else:
                csv_line.append("NR")
                csv_line.append("NR")
                csv_line.append("NR")

            li_for_csv.append(csv_line)

    csv_path = Path(r"./scripts/rouen/service_layer_fix/csv/service_layer_associations.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "service_md_title",
                "service_md_uuid",
                "layer_isogeo_uuid",
                "layer_service_id",
                "layer_title",
                "associated_md_uuid",
                "associated_md_name",
                "associated_md_title",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
