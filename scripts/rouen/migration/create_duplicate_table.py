# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create mapping table for Rouen 2020 migration
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
from datetime import datetime

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

    # Search about all workgroup metadatas because there are less than 800
    whole_md_search = isogeo.search(
        group=workgroup_uuid,
        whole_results=True,
        include="all"
    )
    isogeo.close()

    li_md = [md for md in whole_md_search.results if md.get("name")]

    print("{} metadatas loaded from {} workgroup".format(whole_md_search.total, workgroup_uuid))

    li_md_names_lower = []
    for md in li_md:
        if md.get("name"):
            md_name = md.get("name").lower()
            if md_name not in li_md_names_lower:
                li_md_names_lower.append(md_name)
            else:
                pass
        else:
            pass

    li_for_csv = []
    for md_name in li_md_names_lower:

        li_matching_uuid = [md.get("_id") for md in li_md if md.get("name").lower() == md_name]

        if len(li_matching_uuid) <= 1:
            pass
        elif len(li_matching_uuid) == 2:
            md_1 = [md for md in li_md if md.get("_id") == li_matching_uuid[0]][0]
            md_2 = [md for md in li_md if md.get("_id") == li_matching_uuid[1]][0]

            update_date_1 = datetime.strptime(md_1.get("_modified").split("+")[0][:-1], r"%Y-%m-%dT%H:%M:%S.%f")
            update_date_2 = datetime.strptime(md_2.get("_modified").split("+")[0][:-1], r"%Y-%m-%dT%H:%M:%S.%f")

            matching_quality = ""

            if md_1.get("path") == "sde":
                md_src = md_1
                md_trg = md_2
                if update_date_1 < update_date_2:
                    matching_quality = "1"
                else:
                    matching_quality = "0.5"

            elif md_2.get("path") == "sde":
                md_src = md_2
                md_trg = md_1
                if update_date_2 < update_date_1:
                    matching_quality = "1"
                else:
                    matching_quality = "0.5"
            else:
                print("{} --> impossible to determine wich metadata is the source between {} and {}".format(md_name, md_1.get("_id"), md_1.get("_id")))
            csv_line = [
                md_src.get("_id"),
                md_src.get("title"),
                md_src.get("name"),
                md_trg.get("name"),
                md_trg.get("_id"),
                matching_quality
            ]
            li_for_csv.append(csv_line)
        else:
            pass

    li_fields = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
        "matching_quality"
    ]

    csv_path = Path(r"./scripts/rouen/migration/csv/duplicate_table.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            li_fields
        )
        for data in li_for_csv:
            writer.writerow(data)
