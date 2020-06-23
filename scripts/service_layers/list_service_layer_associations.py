# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create the list of service layers association for a specific workgroup and write it in csv file
    Author:       Isogeo
    Purpose:      Script using isogeo-pysdk to retrieve informations about service layer association.

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
load_dotenv("./env/service_layers.env", override=True)

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

    # Search about workgroup service metadatas
    service_md_search = isogeo.search(
        group=workgroup_uuid, query="type:service", include="all", whole_results=True
    )
    isogeo.close()

    print(
        "{} service metadata retrieved from {} workgroup".format(
            service_md_search.total, workgroup_uuid
        )
    )

    li_for_csv = []

    for md in service_md_search.results:
        service_md_uuid = md.get("_id")
        service_md_title = md.get("title")

        associated_layer_counter = 0
        if len(md.get("layers")):
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
                    associated_layer_counter += 1
                else:
                    csv_line.append("NR")
                    csv_line.append("NR")
                    csv_line.append("NR")

                li_for_csv.append(csv_line)
            print(
                "{}/{} layers associated from '{}' service ({})".format(
                    associated_layer_counter,
                    len(md.get("layers")),
                    service_md_title,
                    service_md_uuid,
                )
            )
        else:
            print(
                "'{}' service ({}) doesn't provide any layer".format(
                    service_md_title, service_md_uuid
                )
            )

    csv_path = Path(environ.get("CSV_FILE_FOLDER")) / "{}_associated_layers.csv".format(
        workgroup_uuid
    )
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
