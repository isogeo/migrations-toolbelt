# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for Normandie 2020 migration
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
load_dotenv("./env/normandie.env", override=True)

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
    src_cat_uuid = environ.get("ISOGEO_CATALOG_SOURCE")
    src_cat_tag = "catalog:{}".format(src_cat_uuid)

    # Search about all workgroup metadatas because there are less than 800
    whole_md_search = isogeo.search(
        group=workgroup_uuid,
        whole_results=True,
        include="all"
    )
    isogeo.close()

    li_md = whole_md_search.results

    print("{} metadatas loaded from {} workgroup".format(whole_md_search.total, workgroup_uuid))

    li_md_alias = []
    for md in li_md:
        if md.get("name"):
            md_alias = md.get("name") + " --> " + md.get("path")
            if md_alias not in li_md_alias:
                li_md_alias.append(md_alias)
            else:
                pass
        else:
            pass

    max_uuid = 0
    li_for_csv = []
    for md_alias in li_md_alias:
        md_name = md_alias.split(" --> ")[0]
        md_path = md_alias.split(" --> ")[1]
        csv_line = [md_alias]

        li_matching_uuid = [md.get("_id") for md in li_md if md.get("name") == md_name and md.get("path") == md_path]
        li_matching_tags = [md.get("tags") for md in li_md if md.get("name") == md_name and md.get("path") == md_path]

        for uuid in li_matching_uuid:
            csv_line.append(uuid)

        if len(li_matching_uuid) > 1:
            matching_type = "trg_matching"
            for tags in li_matching_tags:
                if src_cat_tag in tags:
                    matching_type = "src_matching"
                else:
                    pass
            csv_line.append(matching_type)
        else:
            csv_line.append("no_match")

        if len(li_matching_uuid) > max_uuid:
            max_uuid = len(li_matching_uuid)
        else:
            pass

        li_for_csv.append(csv_line)

    li_fields = ["data_name"]
    for i in range(0, max_uuid):
        li_fields.append("md_uuid_{}".format(i + 1))
    li_fields.append("match_type")

    csv_path = Path(r"./scripts/normandie/csv/mapping.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            li_fields
        )
        for data in li_for_csv:
            writer.writerow(data)