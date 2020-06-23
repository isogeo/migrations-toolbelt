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

    li_duplicate = []
    for md_alias in li_md_alias:
        md_name = md_alias.split(" --> ")[0]
        md_path = md_alias.split(" --> ")[1]
        csv_line = [md_alias]

        li_matching_uuid = [md.get("_id") for md in li_md if md.get("name") == md_name and md.get("path") == md_path]
        li_matching_tags = [md.get("tags") for md in li_md if md.get("name") == md_name and md.get("path") == md_path]

        for uuid in li_matching_uuid:
            csv_line.append(uuid)

        li_duplicate.append(csv_line)

    li_for_csv = []
    for line in li_duplicate:
        new_line = [line[0]]
        li_uuids = line[1:]
        if len(li_uuids) == 1:
            new_line.append(li_uuids[0])
            new_line.append("no_duplicate")
        elif len(li_uuids) == 2:
            md_1 = [md for md in li_md if md.get("_id") == li_uuids[0]][0]
            md_2 = [md for md in li_md if md.get("_id") == li_uuids[1]][0]
            creation_date_1 = datetime.strptime(md_1.get("_created").split("+")[0][:-1], r"%Y-%m-%dT%H:%M:%S.%f")
            creation_date_2 = datetime.strptime(md_2.get("_created").split("+")[0][:-1], r"%Y-%m-%dT%H:%M:%S.%f")
            if creation_date_1 > creation_date_2:
                new_line.append(md_1.get("_id"))
                new_line.append(md_2.get("_id"))
            else:
                new_line.append(md_2.get("_id"))
                new_line.append(md_1.get("_id"))
        elif len(li_uuids) > 2:
            new_line.append(";".join(li_uuids))
            new_line.append("too_much_duplicate")
        else:
            print("PROBLEM : {}".format(line))
            continue
        li_for_csv.append(new_line)

    li_fields = ["data_name", "to_keep", "to_delete"]

    csv_path = Path(r"./scripts/rouen/migration/csv/mapping_bis.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            li_fields
        )
        for data in li_for_csv:
            writer.writerow(data)
