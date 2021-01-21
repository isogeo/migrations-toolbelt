# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         
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
from pprint import pprint

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

    li_involved_wg_uuid = environ.get("ISOGEO_INVOLVED_WORKGROUPS")

    li_wg_infos = [(wg.get("_id"), wg.get("contact").get("name")) for wg in isogeo.workgroup.listing() if wg.get("_id") in li_involved_wg_uuid]

    li_for_csv = []
    for info in li_wg_infos:
        memberships = isogeo.workgroup.memberships(info[0])

        for member in memberships:
            li_for_csv.append(
                [
                    info[1],
                    info[0],
                    "https://manage.isogeo.com/groups/" + info[0],
                    "https://app.isogeo.com/groups/" + info[0] + "/dashboard/formats",
                    member.get("user").get("contact").get("name"),
                    member.get("user").get("contact").get("email"),
                    member.get("user").get("staff"),
                ]
            )

    isogeo.close()

    csv_path = Path("./scripts/misc/workgroup/liste des utilisateurs par GT.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "nom du groupe",
                "UUID du groupe",
                "url du groupe dans manage",
                "url du groupe dans app",
                "nom de l'utilisateur",
                "email de l'utilisateur",
                "staff",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)