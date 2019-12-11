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
import json
from os import environ

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
    src_cat_uuid = environ.get("ISOGEO_CATALOG_SOURCE")
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")

    # SOURCES
    # if checker.check_is_uuid(src_cat_uuid):
    #     src_md = isogeo.search(
    #         group=environ.get("ISOGEO_ORIGIN_WORKGROUP"),
    #         query="catalog:{}".format(src_cat_uuid),
    #         whole_results=True
    #     )
    #     print("{} source metadata loaded".format(src_md.total))

    #     content = src_md.results
    #     with open("scripts/dijon/output_src.json", "w") as outfile:
    #         json.dump(content, outfile, sort_keys=True, indent=4)
    # else:
    #     print("wrong source catalog UUID")

    if checker.check_is_uuid(trg_cat_uuid):
        trg_md = isogeo.search(
            group=environ.get("ISOGEO_ORIGIN_WORKGROUP"),
            query="catalog:{}".format(trg_cat_uuid),
            whole_results=True
        )
        print("{} targets metadata loaded".format(trg_md.total))

        content = trg_md.results
        with open("scripts/dijon/output_trg.json", "w") as outfile:
            json.dump(content, outfile, sort_keys=True, indent=4)
    else:
        print("wrong target catalog UUID")

    isogeo.close()
