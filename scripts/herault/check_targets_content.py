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
load_dotenv("env/herault.env", override=True)

checker = IsogeoChecker()

if __name__ == "__main__":
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    src_cat_uuid = environ.get("ISOGEO_CATALOG_SOURCE")
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")
    migrated_cat_uuid = environ.get("ISOGEO_CATALOG_MIGRATED")

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

    # prepare csv reading
    input_csv = Path(r"./scripts/herault/csv/correspondances_row.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
    ]

    # RETRIEVING INFOS FROM MATCHING TABLE
    li_md_infos = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_uuid = row.get("source_uuid")
            src_title = row.get("source_title")
            src_name = row.get("source_name")
            trg_name = row.get("target_name")
            trg_uuid = row.get("target_uuid")

            if reader.line_num > 1:
                li_md_infos.append(
                    (
                        src_uuid,
                        src_title,
                        src_name,
                        trg_name,
                        trg_uuid,
                    )
                )

    whole_search_md = isogeo.search(
        group=origin_wg_uuid,
        whole_results=True
    ).results

    isogeo.close()

    li_for_csv = []
    # li_for_other_csv = []
    for info in li_md_infos:
        if info[4] != "NR":
            li_trg_md = [md for md in whole_search_md if md.get("_id") == info[4]]
        else:
            li_trg_md = [md for md in whole_search_md if md.get("name") == info[3].strip() and md.get("_id") != info[0]]

        if len(li_trg_md):
            if len(li_trg_md) == 1:
                trg_md = li_trg_md[0]
                li_for_csv.append(
                    [
                        info[0],
                        info[1],
                        info[2],
                        trg_md.get("name"),
                        trg_md.get("_id"),
                        "to_check"
                    ]
                )
            else:
                li_trg_names = "|".join([md.get("name") for md in li_trg_md])
                li_trg_id = "|".join([md.get("_id") for md in li_trg_md])
                li_for_csv.append(
                    [
                        info[0],
                        info[1],
                        info[2],
                        li_trg_names,
                        li_trg_id,
                        "mutliple_target"
                    ]
                )
        else:
            li_for_csv.append(
                [
                    info[0],
                    info[1],
                    info[2],
                    info[3],
                    info[4],
                    "target_not_found"
                ]
            )

    for line in li_for_csv:
        if line[5] != "to_check":
            continue

        trg_md = [md for md in whole_search_md if md.get("_id") == line[4]][0]
        if trg_md.get("title") and "." in trg_md.get("title"):
            trg_md_title = trg_md.get("title").split(".")[1]
        else:
            trg_md_title = trg_md.get("title")
            pass
        if trg_md.get("name") and "." in trg_md.get("name"):
            trg_md_name = trg_md.get("name").split(".")[1]
        else:
            trg_md_name = trg_md.get("name")
            pass

        if (trg_md_title and trg_md_title != trg_md_name) or trg_md.get("abstract"):
            line[5] = "not_empty_target"
            line.extend(
                [
                    "https://app.isogeo.com/groups/" + trg_md.get("_creator").get("_id") + "/resources/" + line[4] + "/identification",
                    "https://app.isogeo.com/groups/" + trg_md.get("_creator").get("_id") + "/resources/" + line[0] + "/identification",
                ]
            )
        else:
            line[5] = "good"
            line.extend(
                [
                    "https://app.isogeo.com/groups/" + trg_md.get("_creator").get("_id") + "/resources/" + line[4] + "/identification",
                    "https://app.isogeo.com/groups/" + trg_md.get("_creator").get("_id") + "/resources/" + line[0] + "/identification",
                ]
            )

    csv_path = Path(r"./scripts/herault/csv/correspondances.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "check",
                "target_app_link",
                "source_app_link",

            ]
        )
        for data in li_for_csv:
            writer.writerow(data)

    # csv_path = Path(r"./scripts/herault/csv/filled_targets.csv")
    # with open(file=csv_path, mode="w", newline="") as csvfile:
    #     writer = csv.writer(csvfile, delimiter=";")
    #     writer.writerow(
    #         [
    #             "source_uuid",
    #             "source_app_link",
    #             "target_uuid",
    #             "target_app_link",
    #         ]
    #     )
    #     for data in li_for_other_csv:
    #         writer.writerow(data)