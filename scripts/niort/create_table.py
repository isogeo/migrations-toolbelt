# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for Niort migration
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
load_dotenv("./env/niort.env", override=True)

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
        Path("./scripts/niort/_logs/create_table_niort.log"), "a", 5000000, 1
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
    src_wg_uuid = environ.get("ISOGEO_SOURCE_WORKGROUP")
    trg_wg_uuid = environ.get("ISOGEO_TARGET_WORKGROUP")
    src_cat_uuid = environ.get("ISOGEO_CATALOG_SOURCE")

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
    # request Isogeo API about source metadatas
    src_cat_search = isogeo.search(
        group=src_wg_uuid,
        query="catalog:{}".format(src_cat_uuid),
        whole_results=True
    )
    logger.info("{} source metadatas retrieved".format(src_cat_search.total))
    # request Isogeo API about target metadatas
    trg_search = isogeo.search(
        group=trg_wg_uuid,
        whole_results=True
    )
    logger.info("{} target metadatas retrieved".format(trg_search.total))
    isogeo.close()
    # retrieve source metadatas infos from Isogeo API response
    li_md_src = []
    for md in src_cat_search.results:
        li_md_src.append((md.get("_id"), md.get("title"), md.get("name", "NR")))
    # retrieve target metadatas infos from Isogeo API response
    li_md_trg = []
    li_name_trg = []
    li_name_trg_low = []
    for md in trg_search.results:
        if md.get("name"):
            li_md_trg.append((md.get("_id"), md.get("name")))
            li_name_trg.append(md.get("name"))
            li_name_trg_low.append(md.get("name").lower())
        else:
            pass
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
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        md_trg[1],
                        md_trg[0],
                        "perfect",
                        "https://app.isogeo.com/groups/" + src_wg_uuid + "/resources/" + md_src[0] + "/identification",
                        "https://app.isogeo.com/groups/" + trg_wg_uuid + "/resources/" + md_trg[0] + "/identification",
                    ]
                )
                nb_matched += 1

            elif md_src[2].lower() in li_name_trg_low:
                index_trg = li_name_trg_low.index(md_src[2].lower())
                md_trg = li_md_trg[index_trg]

                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        md_trg[1],
                        md_trg[0],
                        "incassable",
                        "https://app.isogeo.com/groups/" + src_wg_uuid + "/resources/" + md_src[0] + "/identification",
                        "https://app.isogeo.com/groups/" + trg_wg_uuid + "/resources/" + md_trg[0] + "/identification",
                    ]
                )
                nb_matched += 1

            else:
                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        "NULL",
                        "NULL",
                        "no_match",
                        "https://app.isogeo.com/groups/" + src_wg_uuid + "/resources/" + md_src[0] + "/identification",
                        "NULL",
                    ]
                )
        else:
            li_for_csv.append(
                [
                    md_src[0],
                    md_src[1].replace(";", "<semicolon>"),
                    md_src[2],
                    "NULL",
                    "NULL",
                    "no_match",
                    "https://app.isogeo.com/groups/" + src_wg_uuid + "/resources/" + md_src[0] + "/identification",
                    "NULL",
                ]
            )

    logger.info("{} on {} source metadata have matched with a target".format(nb_matched, len(li_for_csv)))

    csv_path = Path(r"./scripts/niort/csv/correspondances.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "match_type",
                "src_app_link",
                "trg_app_link"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
