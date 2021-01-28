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
        li_md_trg.append((md.get("_id"), md.get("name", "NR")))
        li_name_trg.append(md.get("name", "NR").lower())

    # ############################### LOADING ORIGINAL MATCHING TABLE ###############################
    input_csv = Path(r"./scripts/belfort/csv/correspondances_v0.csv")
    fieldnames = [
        "Nom ORACLE",
        "Nom POSTGRE"
    ]

    # retrieve matching from csv file
    li_from_csv = []
    li_trg_from_csv = []
    li_src_from_csv = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_name = row.get("Nom ORACLE")
            trg_name = row.get("Nom POSTGRE")

            li_from_csv.append((src_name, trg_name))
            li_src_from_csv.append(src_name.lower())
            li_trg_from_csv.append(trg_name.lower())

    # csv matching table content
    li_for_csv = []
    for md in li_md_src:
        src_name = md[2]
        src_title = md[1]
        src_uuid = md[0]

        line_for_csv = [src_uuid, src_title, src_name]

        if src_name != "NR":
            if src_name.lower() in li_src_from_csv:
                matching = [info for info in li_from_csv if info[0].lower() == src_name.lower()][0]
                trg_matching_name = matching[1]
                if trg_matching_name != "NR" and trg_matching_name.lower() in li_name_trg:
                    trg_md = [trg for trg in li_md_trg if trg[1].lower() == trg_matching_name.lower()][0]
                    line_for_csv.append(trg_md[1])
                    line_for_csv.append(trg_md[0])
                else:
                    line_for_csv.append("NR")
                    line_for_csv.append("NR")
            else:
                line_for_csv.append("NR")
                line_for_csv.append("NR")

        else:
            line_for_csv.append("NR")
            line_for_csv.append("NR")

        li_for_csv.append(line_for_csv)

    # retrieving some stats to build report
    nb_matching = len([line for line in li_for_csv if line[4] != "NR"])

    # let's report
    print("{} matching established".format(nb_matching))

    csv_path = Path(r"./scripts/belfort/csv/correspondances_v1ter.csv")
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
