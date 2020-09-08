# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to update matching table for Belfort migration
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
load_dotenv("./env/belfort.env", override=True)

if __name__ == "__main__":
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    src_cat_uuid = environ.get("ISOGEO_CATALOG_SOURCE")
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")
    # ############################### LOADING SOURCE AND TARGET METADATAS INFOS ###############################
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

    # request Isogeo API about source metadatas
    src_cat_search = isogeo.search(
        group=origin_wg_uuid,
        query="catalog:{}".format(src_cat_uuid),
        whole_results=True
    )
    print("{} source metadatas retrieved".format(src_cat_search.total))

    # request Isogeo API about target metadatas
    trg_cat_search = isogeo.search(
        group=origin_wg_uuid,
        query="catalog:{}".format(trg_cat_uuid),
        whole_results=True
    )
    print("{} target metadatas retrieved".format(trg_cat_search.total))

    isogeo.close()

    # retrieve source metadatas infos from Isogeo API response
    li_md_src = []
    li_name_src = []
    for md in src_cat_search.results:
        li_md_src.append((md.get("_id"), md.get("title"), md.get("name", "NR")))
        li_name_src.append(md.get("name", "NR"))

    # retrieve target metadatas infos from Isogeo API response
    li_md_trg = []
    li_name_trg = []
    for md in trg_cat_search.results:
        li_md_trg.append((md.get("_id"), md.get("name")))
        li_name_trg.append(md.get("name", "NR").lower())

    # ############################### LOADING ORIGINAL MATCHING TABLE ###############################
    input_csv = Path(r"./scripts/belfort/csv/correspondances_v0.csv")
    fieldnames = [
        "Nom ORACLE",
        "Nom POSTGRE"
    ]

    li_for_csv = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_name = row.get("Nom ORACLE")
            trg_name = row.get("Nom POSTGRE")

            if src_name in li_name_src and src_name != "NR" and src_name != "Nom ORACLE":
                src_uuid = [tup[0] for tup in li_md_src if tup[2] == src_name][0]
                src_title = [tup[1] for tup in li_md_src if tup[2] == src_name][0]

                if trg_name.lower() in li_name_trg and trg_name != "NR":
                    trg_uuid = [tup[0] for tup in li_md_trg if tup[1].lower() == trg_name.lower()][0]
                    trg_name = [tup[1] for tup in li_md_trg if tup[1].lower() == trg_name.lower()][0]
                else:
                    trg_uuid = "NR"
            elif src_name != "Nom ORACLE":
                src_uuid = "NR"
                src_title = "NR"
                trg_uuid = "NR"
            else:
                continue

            li_for_csv.append(
                [
                    src_uuid,
                    src_title,
                    src_name,
                    trg_name,
                    trg_uuid
                ]
            )

    csv_path = Path(r"./scripts/belfort/csv/correspondances_v1.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
