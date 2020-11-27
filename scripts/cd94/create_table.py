# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for CD94 migration
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

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/cd94.env", override=True)

if __name__ == "__main__":
    # logs
    logger = logging.getLogger()
    # ------------ Log & debug ----------------
    logging.captureWarnings(True)
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.INFO)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler(
        Path("./scripts/cd94/_logs/create_table_cd94.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # ############################### LOADING SOURCE AND TARGET METADATAS INFOS ###############################
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")
    trg_cat_tag = "catalog:{}".format(trg_cat_uuid)
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
    src_cat_md = []
    trg_cat_md = []
    for md in whole_search.results:
        if trg_cat_tag in md.get("tags"):
            trg_cat_md.append(md)
        else:
            src_cat_md.append(md)

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
        li_md_trg.append((md.get("_id"), md.get("name")))
        li_name_trg.append(md.get("name"))
        li_name_trg_low.append(md.get("name").lower())
    # ############################### BUILDING MATCHING TABLE ###############################
    li_for_csv = []
    nb_matched = 0
    for md_src in li_md_src:
        if md_src[2] != "NR":
            if md_src[2] in li_name_trg:
                index_trg = li_name_trg.index(md_src[2])
                md_trg = li_md_trg[index_trg]

                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1],
                        md_src[2],
                        md_trg[1],
                        md_trg[0],
                        "perfect"
                    ]
                )
                nb_matched += 1

            elif md_src[2].lower() in li_name_trg_low:
                index_trg = li_name_trg_low.index(md_src[2].lower())
                md_trg = li_md_trg[index_trg]

                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1],
                        md_src[2],
                        md_trg[1],
                        md_trg[0],
                        "incassable"
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
                        "NR",
                        "NULL"
                    ]
                )
        else:
            li_for_csv.append(
                [
                    md_src[0],
                    md_src[1],
                    md_src[2],
                    "NR",
                    "NR",
                    "NULL"
                ]
            )

    logger.info("{} matches were made between {} source metadatas and {} target metadatas".format(nb_matched, len(src_cat_md), len(trg_cat_md)))

    csv_path = Path(r"./scripts/cd94/csv/correspondances.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "match_type"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
