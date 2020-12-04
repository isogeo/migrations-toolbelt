# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for "Métropole Européenne de Lille" migration
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
load_dotenv("./env/lille.env", override=True)

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
        Path("./scripts/lille/_logs/create_table_lille.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # Shortcuts
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")
    trg_cat_tag = "catalog:{}".format(trg_cat_uuid)
    bound_date = datetime(2020, 11, 22, 0, 0)

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
    isogeo.close()

    # build lists of source and target metadatas
    src_md_search = []
    trg_md_search = []
    # for md in whole_search.results:
    #     if trg_cat_tag in md.get("tags"):
    #         trg_md_search.append(md)
    #     else:
    #         src_md_search.append(md)
    for md in whole_search.results:
        creation_datetime = datetime.fromisoformat(md.get("_created").split("T")[0])
        if creation_datetime < bound_date:
            src_md_search.append(md)
        else:
            trg_md_search.append(md)

    logger.info("{} source metadatas retrieved".format(len(src_md_search)))
    logger.info("{} target metadatas retrieved".format(len(trg_md_search)))
    # retrieve source metadatas infos from Isogeo API response
    li_md_src = []
    for md in src_md_search:
        li_md_src.append((md.get("_id"), md.get("title"), md.get("name", "NR")))
    # retrieve target metadatas infos from Isogeo API response
    li_md_trg = []
    li_name_trg = []
    li_name_trg_low = []
    for md in trg_md_search:
        li_md_trg.append((md.get("_id"), md.get("name", "NR")))
        li_name_trg.append(md.get("name", "NR"))
        li_name_trg_low.append(md.get("name", "NR").lower())

    # ############################### BUILDING MATCHING TABLE ###############################

    li_for_csv = []
    nb_matched = 0
    li_duplicate_src_name = []
    for md_src in li_md_src:
        # if the source metadata has a name we can try to find a matching target
        if md_src[2] != "NR":
            # first, trying to establish case-sensitive match ("perfect" match)
            if md_src[2] in li_name_trg:
                match_count = len([info for info in li_md_src if info[2] == md_src[2]])

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
                        match_count
                    ]
                )
                nb_matched += 1
            # then, trying to establish case-insensitive match ("incassable" match)
            elif md_src[2].lower() in li_name_trg_low:
                match_count = len([info for info in li_md_src if info[2].lower() == md_src[2].lower()])

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
                        match_count
                    ]
                )
                nb_matched += 1
            # if "incassable" and "perfect" matching failed, their is no match
            else:
                match_count = 0
                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1].replace(";", "<semicolon>"),
                        md_src[2],
                        "NR",
                        "NR",
                        "NULL",
                        match_count
                    ]
                )
                continue

            # store duplicate source metadata's name
            if match_count > 1 and md_src[2] not in li_duplicate_src_name:
                li_duplicate_src_name.append(md_src[2])
            else:
                pass

        # if the source metadata has no name the match is impossible
        else:
            li_for_csv.append(
                [
                    md_src[0],
                    md_src[1].replace(";", "<semicolon>"),
                    md_src[2],
                    "NR",
                    "NR",
                    "missing_name",
                    0
                ]
            )

    # Managing duplicate sources : when several source md have the same name

    # First, retrieve all source metadatas info corresponding to each duplicated source name
    for dup_name in li_duplicate_src_name:
        li_dup_md = [md for md in src_md_search if md.get("name", "") == dup_name]
        latest_modified_date = datetime(1950, 1, 1, 0, 0)
        latest_modified_md_uuid = ""
        # Then, compare source md last update date
        for md in li_dup_md:
            modified_date = datetime.fromisoformat(md.get("_modified").split("T")[0])
            if modified_date > latest_modified_date:
                latest_date = latest_modified_date
                latest_modified_md_uuid = md.get("_id")
        # Finally, update csv content to specify which source md gonna be used to perform migration
        for md in li_dup_md:
            if md.get("_id") == latest_modified_md_uuid:
                pass
            else:
                # retrieving corresponding csv line from li_for_csv
                md_line = [line for line in li_for_csv if line[0] == md.get("_id")][0]
                # retrieving corresponding index into li_for_csv
                md_line_index = li_for_csv.index(md_line)
                # updating corresponding line
                li_for_csv[md_line_index][5] = "duplicate"


    logger.info("{} matches were made between {} source metadatas and {} target metadatas".format(nb_matched, len(src_md_search), len(trg_md_search)))

    csv_path = Path(r"./scripts/lille/csv/correspondances_v2.csv")
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
                "match_count"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
