# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to add extraction link to "Métropole Aix Marseille Provence" metadatas
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
import logging
from logging.handlers import RotatingFileHandler
from timeit import default_timer
from pprint import pprint

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, Link
# load .env file
load_dotenv("./env/mamp.env", override=True)

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
        Path("./scripts/misc/events/_logs/add_links.log"),
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

    # shortcut
    url_base = "https://geodata.ampmetropole.fr/?mode_id=extraction&layer="
    wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")

    # Retrieving infos involved metadatas from csv report file
    input_csv = Path(r"./scripts/MAMP/extraction_links/csv/metadatas_names.csv")
    fieldnames = [
        "md_name",
        "file"
    ]
    li_md_names = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)
        for row in reader:
            # if reader.line_num > 1 and row.get("file") == "0":
            if reader.line_num > 1:
                li_md_names.append(
                    (
                        row.get("md_name").strip(),
                        row.get("file")
                    )
                )
            else:
                pass

    # li_md_names = [""]

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

    # md = isogeo.metadata.get("8ea16a7b7ab042fc8156f6431efaf5ec")
    # print(isogeo.metadata.links.listing(md))

    # ask Isogeo API about whole MAMP metadatas
    whole_search = isogeo.search(
        whole_results=True,
        group=wg_uuid
    )
    # retrieve the dedicated catalog object
    # cat = isogeo.catalog.get(
    #     workgroup_id=wg_uuid,
    #     catalog_id="d220c42c6b4c4dbd8b068dde32579b58"
    # )

    logger.info("{} metadata retrieved from {} workgroup".format(whole_search.total, wg_uuid))

    # filter involved metadatas
    li_md = [md for md in whole_search.results if md.get("name")]
    # li_md_to_parse_infos = [(md.get("_id"), md.get("name")) for md in li_md if md.get("name") in li_md_names and r"\\" not in r"{}".format(md.get("name"))]
    li_md_to_parse_infos = [(md.get("_id"), md.get("name"), md.get("path")) for md in li_md]

    li_for_csv = []
    nb_matchs = 0
    for tup in li_md_names:
        # for database table
        if tup[1] == "0":
            data_type = "table"
            name = tup[0]
            md_infos = [info for info in li_md_to_parse_infos if info[1].lower() == name.lower()]
        # for data files
        else:
            data_type = "fichier"
            name = tup[0].split("\\")[-1]
            md_infos = [info for info in li_md_to_parse_infos if name in info[2]]

        if len(md_infos) == 1:
            nb_matchs += 1
            # refresh token if needed
            if default_timer() - auth_timer >= 6900:
                logger.info("Manually refreshing token")
                isogeo.connect(
                    username=environ.get("ISOGEO_USER_NAME"),
                    password=environ.get("ISOGEO_USER_PASSWORD"),
                )
                auth_timer = default_timer()
            else:
                pass

            # retrieve metadata object
            md = isogeo.metadata.get(md_infos[0][0])

            # build extraction URL from data name
            extraction_url = url_base + name

            # build link object
            extraction_link = Link()
            extraction_link.title = "Extraire la donnée"
            extraction_link.url = extraction_url
            extraction_link.type = "url"
            extraction_link.kind = "data"
            extraction_link.action = ["download"]

            # # add the link to the metadata
            # isogeo.metadata.links.create(md, extraction_link)

            # # associate the metadata to the dedicated catalog
            # isogeo.catalog.associate_metadata(md, cat)

            li_for_csv.append(
                [
                    tup[0],
                    data_type,
                    md._id,
                    "https://app.isogeo.com/groups/" + wg_uuid + "/resources/" + md._id,
                    extraction_url
                ]
            )
        elif len(md_infos) == 0:
            li_for_csv.append(
                [
                    tup[0],
                    data_type,
                    "no_match",
                    "NR",
                    "NR"
                ]
            )
        else:
            li_for_csv.append(
                [
                    tup[0],
                    data_type,
                    "multiple_match",
                    "NR",
                    "NR"
                ]
            )

    isogeo.close()

    logger.info("{} matches established".format(nb_matchs))

    csv_path = Path(r"./scripts/MAMP/extraction_links/csv/extraction_link_report.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "layer_id",
                "Type de donnée",
                "UUID de la métadonnée Isogeo",
                "Lien vers la fiche dans app",
                "Lien d'extraction"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
