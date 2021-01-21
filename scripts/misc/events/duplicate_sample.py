# -*- coding: UTF-8 -*-
#! python3

# ##############################################################################
# ########## Libraries #############

# Standard Library
from os import environ
from timeit import default_timer
import csv
from pathlib import Path
from random import sample

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load dijon.env file
load_dotenv("./env/events.env", override=True)

if __name__ == "__main__":

    input_csv = Path(r"./scripts/misc/events/csv/dataPath_cleaner_1606305212.csv")
    fieldnames = [
        "wg_name",
        "wg_uuid",
        "md_uuid",
        "event_uuid",
        "event_description",
        "event_description_light",
        "operation",
    ]
    li_to_clean = []
    li_to_delete = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            md_uuid = row.get("md_uuid")
            operation = row.get("operation")
            if operation == "cleaned" and md_uuid not in li_to_clean:
                li_to_clean.append(md_uuid)
            elif operation == "deleted" and md_uuid not in li_to_delete:
                li_to_delete.append(md_uuid)
            else:
                pass

    li_md_sample = []
    for uuid in sample(li_to_clean, 10):
        li_md_sample.append(uuid)
    for uuid in sample(li_to_delete, 10):
        li_md_sample.append(uuid)

    # ############################### MIGRATING ###############################
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

    dest_cat = isogeo.catalog.get(workgroup_id="2f97fc44ac324d29a59ffa1ffbca080c", catalog_id="a9310b1114924f2c83cb5a30c91d8f83")

    li_dest_uuid = []

    for md_uuid in li_md_sample:

        if default_timer() - auth_timer >= 6900:
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        # retrieve origin metadata
        src_migrator = MetadataDuplicator(
            api_client=isogeo, source_metadata_uuid=md_uuid
        )
        src_loaded = src_migrator.metadata_source

        # not import origin catalogs
        keys_to_remove = []
        for tag in src_loaded.tags:
            if tag.startswith("catalog:"):
                keys_to_remove.append(tag)
        for key in keys_to_remove:
            del src_loaded.tags[key]

        # import metadata into destinatin workgroup
        md_dst = src_migrator.duplicate_into_other_group(
            destination_workgroup_uuid="2f97fc44ac324d29a59ffa1ffbca080c",
            copymark_abstract=True,
            copymark_title=True
        )

        # associate destination metadata to catalog
        isogeo.catalog.associate_metadata(metadata=md_dst, catalog=dest_cat)
        li_dest_uuid.append(md_dst._id)

    isogeo.close()
    print(li_dest_uuid)
