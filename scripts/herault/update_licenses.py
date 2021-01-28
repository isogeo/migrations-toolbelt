# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to update Herault metadata licenses
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
from os import environ
from pathlib import Path
from pprint import pprint

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    Condition,
    Metadata
)

# load .env file
load_dotenv("./env/herault.env", override=True)

if __name__ == "__main__":
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
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

    lic_etalab1_uuid = "63f121e14eda4f47b748595e0bcccc31"
    lic_etalab2_uuid = "f6e0c665905a4feab1e9c1d6359a225f"
    lic_etalab1 = isogeo.license.get(lic_etalab1_uuid)
    lic_etalab2 = isogeo.license.get(lic_etalab2_uuid)

    new_condition = Condition()
    new_condition._license = lic_etalab2

    search_migrated = isogeo.search(
        group=origin_wg_uuid,
        query="catalog:{}".format(migrated_cat_uuid),
        whole_results=True,
        include=("conditions",)
    )

    for md in search_migrated.results:
        # retrieve licenses'ids of metadata's conditions
        md_lic_uuid = [condition.get("license").get("_id") for condition in md.get("conditions")]
        # tests if Etalab is one of metadata conditions' license
        if len(md_lic_uuid) and lic_etalab1_uuid in md_lic_uuid:
            # retrieve condition to delete dict
            md_condition = [condition for condition in md.get("conditions") if condition.get("license").get("_id") == lic_etalab1_uuid][0]

            # build metadata object
            isogeo_md = Metadata(**md)

            # add condtion to delete descritpion to new condition before adding new condition
            new_condition._description = md_condition.get("description")
            isogeo.metadata.conditions.create(metadata=isogeo_md, condition=new_condition)

            # build condition to delete object before deleting it
            isogeo_condition = Condition(**md_condition)
            isogeo.metadata.conditions.delete(metadata=isogeo_md, condition=isogeo_condition)
        else:
            pass
