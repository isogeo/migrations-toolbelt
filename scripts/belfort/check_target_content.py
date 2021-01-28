# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         A Script to check if metadatas (uuid retrieved from a csv file) are empty or filled
    Author:       Isogeo
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
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator, BackupManager

# load dijon.env file
load_dotenv("env/belfort.env", override=True)

checker = IsogeoChecker()

if __name__ == "__main__":

    li_infos = []
    # prepare csv reading
    input_csv = Path(r"./scripts/belfort/csv/correspondances.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
        "match_type"
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_uuid = row.get("source_uuid")
            trg_uuid = row.get("target_uuid")
            if reader.line_num > 1 and trg_uuid != "NR":
                li_infos.append(
                    (
                        src_uuid,
                        trg_uuid
                    )
                )
            else:
                pass

    # ############################### MIGRATING ###############################
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

    li_for_csv = []
    for info in li_infos:
        # Manually refreshing token if needed
        if default_timer() - auth_timer >= 250:
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        md = isogeo.metadata.get(info[1])
        if md.title and "." in md.title:
            md_title = md.title.split(".")[1]
        else:
            md_title = md.title
            pass
        if md.name and "." in md.name:
            md_name = md.name.split(".")[1]
        else:
            md_name = md.name
            pass
        if (md_title and md_title != md_name) or md.abstract:
            li_for_csv.append(
                [
                    info[0],
                    "https://app.isogeo.com/groups/" + md._creator.get("_id") + "/resources/" + info[0] + "/identification",
                    info[1],
                    "https://app.isogeo.com/groups/" + md._creator.get("_id") + "/resources/" + info[1] + "/identification"
                ]
            )
        else:
            pass

    csv_path = Path(r"./scripts/belfort/csv/filled_targets.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "app_link_source",
                "target_uuid",
                "app_link_target"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)