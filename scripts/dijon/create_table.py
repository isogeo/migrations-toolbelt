# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for Jura data in 2019
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import json
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
load_dotenv(".env", override=True)

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

    li_md_src = []
    with open("scripts/dijon/output_src.json", "r") as src_file:
        src_md = json.load(src_file)
        for md in src_md:
            li_md_src.append((md.get("_id"), md.get("title"), md.get("name")))

    li_name_trg = []
    li_md_trg = []
    with open("scripts/dijon/output_trg.json", "r") as trg_file:
        trg_md = json.load(trg_file)
        for md in trg_md:
            li_md_trg.append((md.get("_id"), md.get("name")))
            li_name_trg.append(md.get("name"))

    li_for_csv = []
    nb_matched = 0
    for md_src in li_md_src:
        if md_src[2] in li_name_trg:
            index_trg = li_name_trg.index(md_src[2])
            md_trg = li_md_trg[index_trg]
            li_for_csv.append(
                [
                    md_src[0],
                    md_src[1],
                    md_src[2],
                    md_trg[1],
                    md_trg[0]

                ]
            )
            nb_matched += 1
        else:
            li_for_csv.append(
                [
                    md_src[0],
                    md_src[1],
                    md_src[2],
                    "NR",
                    "NR"

                ]
            )
            pass

    print("{} on {} source metadata have matched with a target".format(nb_matched, len(li_for_csv)))

    csv_path = Path(r"./scripts/dijon/correspondances.csv")
    with open(file=csv_path, mode="w", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)