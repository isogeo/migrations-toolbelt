# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to associate service layers to metadatas according to a csv file 
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
from timeit import default_timer

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

    # retrieve information about association between layer and metadata from csv file
    dict_to_associate = {}

    input_csv = Path(r"./scripts/rouen/service_layer_fix/csv/for_service_layer_associations.csv")
    fieldnames = [
        "service_md_title",
        "service_md_uuid",
        "layer_isogeo_uuid",
        "layer_service_id",
        "layer_title",
        "associated_md_uuid",
        "associated_md_name",
        "associated_md_title"
    ]

    # store informations into a dict
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            srv_md_uuid = row.get("service_md_uuid")
            if srv_md_uuid != "service_md_uuid" and row.get("associated_md_uuid") != "NR":  # EN PROD
                tup_info = (
                    row.get("layer_isogeo_uuid"),
                    row.get("layer_service_id"),
                    row.get("layer_title"),
                    row.get("associated_md_uuid"),
                    row.get("associated_md_name"),
                    row.get("associated_md_title")
                )
                if srv_md_uuid in list(dict_to_associate.keys()):    
                    dict_to_associate[srv_md_uuid].append(
                        tup_info
                    )
                else:
                    dict_to_associate[srv_md_uuid] = [
                        tup_info
                    ]

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
    auth_timer = default_timer()

    count = 0
    for srv_md_uuid in dict_to_associate:
        li_layer_infos = dict_to_associate[srv_md_uuid]
        srv_md = isogeo.metadata.get(srv_md_uuid, include="all")

        for layer in li_layer_infos:
            # Manually refresh token if needed
            if default_timer() - auth_timer >= 230:
                print("Manually refreshing token")
                isogeo.connect(
                    username=environ.get("ISOGEO_USER_NAME"),
                    password=environ.get("ISOGEO_USER_PASSWORD"),
                )
                auth_timer = default_timer()
            else:
                pass

            # retrieve informations about metadata we want to associate service layer with
            trg_md_uuid = layer[3]
            trg_md_name = layer[4]
            trg_md_title = layer[5]
            trg_md = isogeo.metadata.get(trg_md_uuid)

            # retrieve information about service layer we want to associate
            lyr_to_assoc_id = layer[1]
            lyr_to_assoc_title = layer[2]
            lyr_to_assoc_uuid = [lyr.get("_id") for lyr in srv_md.layers if lyr.get("id") == lyr_to_assoc_id and lyr.get("titles")[0].get("value") == lyr_to_assoc_title][0]
            lyr_to_assoc = isogeo.metadata.layers.layer(metadata_id=srv_md_uuid, layer_id=lyr_to_assoc_uuid)

            # associate service layer to metadata
            isogeo.metadata.layers.associate_metadata(
                service=srv_md,
                layer=lyr_to_assoc,
                dataset=trg_md
            )
            count += 1
    print("{} service layers successfuly associated to metadata sheets".format(count))
    isogeo.close()
