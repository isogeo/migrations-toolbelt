# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for Rouen 2020 migration
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
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")

    # Search about all workgroup metadatas because there are less than 800
    whole_md_search = isogeo.search(
        group=workgroup_uuid,
        whole_results=True
    )
    isogeo.close()

    print("{} metadatas loaded from {} workgroup".format(whole_md_search.total, workgroup_uuid))

    trg_cat_tag = "catalog:{}".format(trg_cat_uuid)

    li_src_md = whole_md_search.results
    li_trg_md = [md for md in whole_md_search.results if trg_cat_tag in list(md.get("tags").keys())]

    for md in li_trg_md:
        li_src_md.remove(md)

    print("{} metadatas found into target catalog".format(len(li_trg_md)))
    print("{} potential source metadatas".format(len(li_src_md)))

    if len(li_trg_md) + len(li_src_md) != whole_md_search.total:
        print("There is a problem because some metadatas doesn't appears in sources or targets")
    else:
        for md in li_trg_md:
            md_tags = md.get("tags")
            if trg_cat_tag not in list(md_tags.keys()):
                print("There is a problem because {} metadata should not be considered as a target".format(md.get("_id")))
                break
            else:
                pass
        for md in li_src_md:
            md_tags = md.get("tags")
            if trg_cat_tag in list(md_tags.keys()):
                print("There is a problem because {} metadata should be considered as a source".format(md.get("_id")))
                break
            else:
                pass

    li_src = []
    for md in li_src_md:
        src_infos = (md.get("_id"), md.get("title", "NR"), md.get("name", "NR"))
        li_src.append(src_infos)

    li_trg = []
    li_trg_name = []
    for md in li_trg_md:
        li_trg_name.append(md.get("name").lower())
        trg_infos = (md.get("_id"), md.get("name"))
        li_trg.append(trg_infos)

    li_for_csv = []
    nb_matchs = 0
    for md in li_src:
        src_name = md[2].lower()
        if src_name in li_trg_name:
            trg_infos = [md_infos for md_infos in li_trg if md_infos[1].lower() == src_name][0]
            li_for_csv.append(
                (
                    md[0],
                    md[1],
                    md[2],
                    trg_infos[1],
                    trg_infos[0]
                )
            )
            nb_matchs += 1
        else:
            li_for_csv.append(
                (
                    md[0],
                    md[1],
                    md[2],
                    "no_match",
                    "no_match"
                )
            )

    print("{} potential source metadata match with a target metadata".format(nb_matchs))

    csv_path = Path(r"./scripts/rouen/csv/correspondances.csv")
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
