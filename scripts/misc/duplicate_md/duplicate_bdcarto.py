# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to...
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
from isogeo_pysdk import Isogeo, Condition, Metadata

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load .env file
load_dotenv("./env/.env", override=True)

# #############################################################################
# ########## Main program ###############
# #######################################

if __name__ == "__main__":

    # Shortcuts
    wg_uuid = environ.get("ISOGEO_IGN_WORKGROUP")

    bdcarto_trg_cat_uuid = environ.get("ISOGEO_IGN_BDCARTO_TRG_CAT")
    bdcarto_src_cat_uuid = environ.get("ISOGEO_IGN_BDCARTO_SRC_CAT")

    li_cat_uuid = environ.get("ISOGEO_IGN_INVOLVED_CATALOGS").split(";")
    # li_cat_uuid = [bdcarto_src_cat_uuid]

    license_uuid_src = "34f800d2370a43d2a1681eb2397b0dd3"
    license_uuid_trg = "f6e0c665905a4feab1e9c1d6359a225f"

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

    license_trg = isogeo.license.get(license_uuid_trg)

    new_condition = Condition()
    new_condition.license = license_trg

    wg_search = isogeo.search(
        group=wg_uuid,
        whole_results=True,
        include="all"
    )

    # pprint(wg_search.results)
    for cat_uuid in li_cat_uuid:
        cat_tag = "catalog:{}".format(cat_uuid)
        cat_md = [md for md in wg_search.results if cat_tag in md.get("tags")]
        # cat_md = [md for md in wg_search.results if md.get("_id") == "e3f98e8b65f14ff2ab33a782a3717272"]

        for md in cat_md:
            # refresh token if needed
            if default_timer() - auth_timer >= 6900:
                isogeo.connect(
                    username=environ.get("ISOGEO_USER_NAME"),
                    password=environ.get("ISOGEO_USER_PASSWORD"),
                )
                auth_timer = default_timer()
            else:
                pass

            metadata = Metadata(**md)
            # retrieve the old condition
            li_cond_to_delete = [cond for cond in md.get("conditions") if cond.get("license").get("_id") == license_uuid_src]
            if len(li_cond_to_delete):
                # build the old condition
                condition_to_delete = Condition(**li_cond_to_delete[0])
                # add the new condition
                isogeo.metadata.conditions.create(
                    metadata=metadata,
                    condition=new_condition
                )
                # delete the old condition
                isogeo.metadata.conditions.delete(
                    metadata=metadata,
                    condition=condition_to_delete
                )
            else:
                pass

            # duplicate md if required
            if cat_uuid == bdcarto_src_cat_uuid:
                duplicator = MetadataDuplicator(
                    api_client=isogeo,
                    source_metadata_uuid=md.get("_id")
                )
                duplicator.duplicate_into_same_group(
                    copymark_catalog=bdcarto_trg_cat_uuid,
                    copymark_title=False,
                    copymark_abstract=False,
                    exclude_catalogs=[bdcarto_src_cat_uuid],
                    switch_service_layers=True
                )
            else:
                pass
    isogeo.close()
