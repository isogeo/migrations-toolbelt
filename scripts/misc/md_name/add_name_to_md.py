# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to add a name to IGN-F metadatas manually created.
    Author:       Isogeo
    Purpose:      Script using isogeo-pysdk to update events.

    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
from os import environ
from pathlib import Path
from timeit import default_timer
from datetime import datetime

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, Metadata

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load .env file
load_dotenv("./env/.env", override=True)

# #############################################################################
# ########## Main program ###############
# #######################################

if __name__ == "__main__":

    # Retrieving infos about corrupted events from csv report file
    input_csv = Path(r"./scripts/misc/md_name/csv/name_to_add.csv")
    fieldnames = [
        "wg_uuid",
        "md_uuid",
        "md_name",
    ]
    li_info = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            if reader.line_num > 1:
                li_info.append(
                    (
                        row.get("wg_uuid"),
                        row.get("md_uuid"),
                        row.get("md_name"),
                    )
                )
            else:
                pass
    nb_to_parse = len(li_info)

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

    li_parsed = []
    for info in li_info:

        if default_timer() - auth_timer >= 6900:
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        migrator = MetadataDuplicator(
            api_client=isogeo, source_metadata_uuid=info[1]
        )

        migrator.metadata_source.name = info[2]

        duplicated = migrator.duplicate_into_same_group(
            copymark_abstract=False,  # FALSE EN PROD
            copymark_title=False,  # FALSE EN PROD
            copymark_catalog="5c743581c7724f69bdab6b542d003b7a",
            switch_service_layers=True
        )

        li_parsed.append(
            [
                duplicated.title,
                duplicated.name,
                info[1],
                duplicated._id,
                "https://app.isogeo.com/groups/" + info[0] + "/resources/" + info[1] + "/identification",
                "https://app.isogeo.com/groups/" + info[0] + "/resources/" + duplicated._id + "/identification",
            ]
        )

    isogeo.close()

    for parsed in li_parsed:
        print("{} - {} - {}".format(parsed[0], parsed[1], parsed[2]))

    csv_path = Path("./scripts/misc/md_name/csv/migrated.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "md_title",
                "md_name",
                "src_uuid",
                "trg_uuid",
                "src_app_link",
                "trg_app_link"
            ]
        )
        for data in li_parsed:
            writer.writerow(data)
