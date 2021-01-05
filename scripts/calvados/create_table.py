# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for "Département du Calvados" workgroup metadata migration
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from datetime import datetime, timezone

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/calvados.env", override=True)

if __name__ == "__main__":
    # logs
    logger = logging.getLogger()
    # ------------ Log & debug ----------------
    logging.captureWarnings(True)
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler(
        Path("./scripts/calvados/_logs/create_table_calvados.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.DEBUG)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # Shortcuts
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    bound_date = datetime(2020, 10, 20, 0, 0)
    # bound_date = datetime(2020, 12, 9, 0, 0)

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

    # request Isogeo API about metadatas
    whole_search = isogeo.search(
        group=origin_wg_uuid,
        whole_results=True
    )

    # build lists of source and target metadatas
    src_cat_md = []
    trg_cat_md = []
    for md in whole_search.results:
        creation_datetime = datetime.fromisoformat(md.get("_created").split("T")[0])
        if creation_datetime < bound_date:
            src_cat_md.append(md)
        else:
            trg_cat_md.append(md)

    logger.info("{} source metadatas retrieved".format(len(src_cat_md)))
    logger.info("{} target metadatas retrieved".format(len(trg_cat_md)))
    isogeo.close()
    # retrieve source metadatas infos from Isogeo API response
    li_md_src = []
    for md in src_cat_md:
        li_md_src.append((md.get("_id"), md.get("title"), md.get("name", "NR")))
    # retrieve target metadatas infos from Isogeo API response
    li_md_trg = []
    li_name_trg = []
    li_name_trg_low = []
    for md in trg_cat_md:
        li_md_trg.append((md.get("_id"), md.get("name", "NR"), md.get("_created").split("T")[0]))
        li_name_trg.append(md.get("name", "NR"))
        li_name_trg_low.append(md.get("name", "NR").lower())

    # ############################### BUILDING MATCHING TABLE ###############################

    li_for_csv = []
    nb_matched = 0
    for md_src in li_md_src:
        src_app_link = "https://app.isogeo.com/groups/" + origin_wg_uuid + "/resources/" + md_src[0]
        if md_src[2] != "NR":
            if md_src[2] in li_name_trg:
                match_count = len([info for info in li_md_src if info[2] == md_src[2]])

                index_trg = li_name_trg.index(md_src[2])
                md_trg = li_md_trg[index_trg]

                if match_count == 1:
                    trg_app_link = "https://app.isogeo.com/groups/" + origin_wg_uuid + "/resources/" + md_trg[0]
                else:
                    trg_app_link = "multiple_match"

                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        md_trg[1],
                        md_trg[0],
                        md_trg[2],
                        "perfect",
                        match_count,
                        src_app_link,
                        trg_app_link
                    ]
                )
                nb_matched += 1

            elif md_src[2].lower() in li_name_trg_low:
                match_count = len([info for info in li_md_src if info[2].lower() == md_src[2].lower()])

                index_trg = li_name_trg_low.index(md_src[2].lower())
                md_trg = li_md_trg[index_trg]

                if match_count == 1:
                    trg_app_link = "https://app.isogeo.com/groups/" + origin_wg_uuid + "/resources/" + md_trg[0]
                else:
                    trg_app_link = "multiple_match"

                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        md_trg[1],
                        md_trg[0],
                        md_trg[2],
                        "incassable",
                        match_count,
                        src_app_link,
                        trg_app_link
                    ]
                )
                nb_matched += 1

            else:
                trg_app_link = "NULL"

                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        "NULL",
                        "NULL",
                        "NULL",
                        "no_match",
                        0,
                        src_app_link,
                        trg_app_link
                    ]
                )
        else:
            trg_app_link = "NULL"

            li_for_csv.append(
                [
                    md_src[0],
                    md_src[1].replace(";", "<semicolon>"),
                    md_src[2],
                    "NULL",
                    "NULL",
                    "NULL",
                    "missing_name",
                    0,
                    src_app_link,
                    trg_app_link
                ]
            )

    logger.info("{} matches were made between {} source metadatas and {} target metadatas".format(nb_matched, len(src_cat_md), len(trg_cat_md)))

    csv_path = Path(r"./scripts/calvados/csv/correspondances.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "target_creation_date",
                "match_type",
                "match_count",
                "source_app_link",
                "target_app_link",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
