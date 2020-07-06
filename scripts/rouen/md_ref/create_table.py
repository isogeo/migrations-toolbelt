# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for Rouen 2020 reference meadata service
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
from timeit import default_timer

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker, Catalog

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/rouen.env", override=True)

if __name__ == "__main__":
    # instanciate log
    # logs
    logger = logging.getLogger()
    # ------------ Log & debug ----------------
    logging.captureWarnings(True)
    logger.setLevel(logging.INFO)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler(
        Path("./scripts/rouen/md_ref/_logs/create_table_md_ref_rouen.log"),
        "a",
        5000000,
        1,
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # Retrieve informations about Isogeo ressources from .env file
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    isogeo_wg_uuid = environ.get("ISOGEO_IGNF_WORKGROUP")
    li_isogeo_cat_uuids = [
        environ.get("IGNF_BDTOPO_CAT"),
        environ.get("IGNF_BDCARTO_CAT"),
        environ.get("IGNF_ADMINEXPRESS_CAT"),
        environ.get("IGNF_ROUTE500_CAT"),
    ]

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

    # retrieve destination worgroup metadata
    destination_search = isogeo.search(group=origin_wg_uuid, whole_results=True)
    li_dest_md = destination_search.results
    origin_wg_name = li_dest_md[0].get("_creator").get("contact").get("name")
    logger.info("{} metadatas retrieved from '{}' destination workgroup ({})".format(destination_search.total, origin_wg_name, origin_wg_uuid))

    # retrieve isogeo catalogs infos
    li_isogeo_wg_cat = isogeo.catalog.listing(workgroup_id=isogeo_wg_uuid, include="all")
    li_isogeo_cat = []
    for cat in li_isogeo_wg_cat:
        if cat.get("_id") in li_isogeo_cat_uuids:
            li_isogeo_cat.append(Catalog.clean_attributes(cat))
        else:
            pass

    # Let's prepare csv content, searching for matching into each catalog md
    li_for_csv = []
    # retrieve matching for each cat to migrate
    for cat in li_isogeo_cat:
        # manually refreshing token if needed
        if default_timer() - auth_timer >= 230:
            logger.info("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        # retrieve isogeo cat md
        src_cat_search = isogeo.search(
            group=isogeo_wg_uuid, query="catalog:{}".format(cat._id), whole_results=True
        )
        li_src_md = src_cat_search.results
        # retrieve isogeo catalog info
        logger.info(
            "{} metadata retrieved from '{}' ({}) catalog of '{}' ({}) workgroup".format(
                src_cat_search.total,
                cat.name,
                cat._id,
                cat.owner.get("contact").get("name"),
                cat.owner.get("contact").get("_id"),
            )
        )

        # search for matching for each source md retrieved
        for src_md in li_src_md:
            if src_md.get("name"):
                line_for_csv = [
                    cat.name,
                    src_md.get("_id"),
                    src_md.get("title"),
                    src_md.get("name"),
                ]
                li_matching_md = [
                    md for md in li_dest_md if md.get("name") == src_md.get("name")
                ]
                # no match
                if len(li_matching_md) == 0:
                    line_for_csv.append("NR")
                    line_for_csv.append("NR")
                    line_for_csv.append("no_match")
                # single match
                elif len(li_matching_md) == 1:
                    matching_md = li_matching_md[0]
                    line_for_csv.append(matching_md.get("name"))
                    line_for_csv.append(matching_md.get("_id"))
                    line_for_csv.append("single_match")
                # multiple match
                elif len(li_matching_md) > 1:
                    li_matching_names = [md.get("name") for md in li_matching_md]
                    li_matching_uuids = [md.get("_id") for md in li_matching_md]
                    line_for_csv.append(";".join(li_matching_names))
                    line_for_csv.append(";".join(li_matching_uuids))
                    line_for_csv.append("multiple_match")
                else:
                    logger.info("Unexpected matching case found : {}".format(li_matching_md))
            else:
                line_for_csv = [
                    cat.name,
                    src_md.get("_id"),
                    src_md.get("title"),
                    "NR",
                    "NR",
                    "NR",
                    "to_found_manually"
                ]

            li_for_csv.append(line_for_csv)

    isogeo.close()

    # informe the user with some stats about matching for each catalog
    for cat in li_isogeo_cat:
        li_cat_lines = [line for line in li_for_csv if line[0] == cat.name]
        nb_no_match = len([line for line in li_cat_lines if line[6] == "no_match"])
        nb_single_match = len(
            [line for line in li_cat_lines if line[6] == "single_match"]
        )
        nb_multiple_match = len(
            [line for line in li_cat_lines if line[6] == "to_found_manually"]
        )
        nb_manual_match = len(
            [line for line in li_cat_lines if line[6] == "multiple_match"]
        )
        logger.info(
            "On the {} metadata(s) retrieved from '{}' catalog, the script found : {} no match(s), {} single match(s) and {} multiple match(s)".format(
                len(li_cat_lines),
                cat.name,
                nb_no_match,
                nb_single_match,
                nb_multiple_match,
            )
        )
        if nb_manual_match > 0:
            logger.info("{} match(s) have to be found manually".format(nb_manual_match))
        else:
            pass

    # let's write csv file now the content is ready
    csv_path = Path(r"./scripts/rouen/md_ref/csv/correspondances.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "isogeo_cat",
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "match_type",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
