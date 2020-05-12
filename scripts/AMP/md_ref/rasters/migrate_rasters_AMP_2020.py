# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for Cadastre Geomap migration for AMP (2020)
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# ##############################################################################
# ########## Libraries #############

# Standard Library
import logging
import csv
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator, BackupManager

# load .env file
load_dotenv("./env/amp.env", override=True)

checker = IsogeoChecker()

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
        Path("./scripts/AMP/md_ref/_logs/migration_cadsatregeomap.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("\n######################## MIGRATION SESSION ########################")
    logger.info("-------------- RETRIEVING INFOS FROM ISOGEO API ------------------")

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
    # ################# RETRIEVE UUID FROM ISOGEO API REQUEST #################
    ign_wg_uuid = environ.get("ISOGEO_IGN_WORKGROUP")
    raster_cat_uuid = environ.get("IGN_RASTER_CATALOG_UUID")

    raster_cat_search = isogeo.search(
        group=ign_wg_uuid,
        query="catalog:{}".format(raster_cat_uuid),
        whole_results=True
    )
    logger.info("{} metadata retrieved to import into target work group".format(raster_cat_search.total))
    # to store source metadata uuid
    li_uuid_to_migrate = []
    # prepare csv reading
    for md in raster_cat_search.results:
        src_uuid = md.get("_id")
        # check source UUID validity
        if not checker.check_is_uuid(src_uuid):
            logger.info(
                "{} source UUID isn't valid".format(src_uuid)
            )
        else:
            li_uuid_to_migrate.append(src_uuid)

    li_uuid_to_migrate = ["96cd73589f474565b35ce59b12633c3b"]  # #############################################################
    # ############################### MIGRATING ###############################
    logger.info(
        "{} metadatas will be migrated".format(
            len(li_uuid_to_migrate)
        )
    )
    # ------------------------------------ BACKUP --------------------------------------
    if int(environ.get("BACKUP")) == 1 and len(li_uuid_to_migrate) > 0:
        logger.info("---------------------------- BACKUP {} metadatas---------------------------------".format(len(li_uuid_to_migrate)))
        # backup manager instanciation
        backup_path = Path(r"./scripts/AMP/md_ref/rasters/_output/_backup")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50

        if len(li_uuid_to_migrate) > amplitude:
            bound_range = int(len(li_uuid_to_migrate) / amplitude)
            li_bound = []
            for i in range(bound_range + 1):
                li_bound.append(amplitude * i)
            li_bound.append(len(li_uuid_to_migrate))

            logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
            for i in range(len(li_bound) - 1):
                if default_timer() - auth_timer >= 250:
                    logger.info("Manually refreshing token")
                    backup_mng.isogeo.connect(
                        username=environ.get("ISOGEO_USER_NAME"),
                        password=environ.get("ISOGEO_USER_PASSWORD"),
                    )
                    auth_timer = default_timer()
                else:
                    pass
                bound_inf = li_bound[i]
                bound_sup = li_bound[i + 1]
                logger.info("Round {} - backup from source metadata {} to {}".format(i + 1, bound_inf + 1, bound_sup))

                search_parameters = {"query": None, "specific_md": tuple(li_uuid_to_migrate[bound_inf:bound_sup])}
                try:
                    backup_mng.metadata(search_params=search_parameters)
                except Exception as e:
                    logger.info("an error occured : {}".format(e))
        else:
            search_parameters = {"query": None, "specific_md": tuple(li_uuid_to_migrate)}
            backup_mng.metadata(search_params=search_parameters)
    else:
        pass

    # ----------------------------------- MIGRATING ------------------------------------
    logger.info("--------------------------- MIGRATING {} metadatas -------------------------------".format(len(li_uuid_to_migrate)))

    # RETRIEVING SOME SUBRESSOURCES WE WANT OR DON'T WANT TO ASSOCIATE TARGET METADATAS WITH 
    # retrieve source workgroup catalogs we don't want to associate target metadatas with
    li_cat_to_exclude = []
    cat_search = isogeo.catalog.listing(workgroup_id=ign_wg_uuid)
    for cat in cat_search:
        li_cat_to_exclude.append(cat.get("_id"))
    logger.info("{} source catalogs to exclude retrieved".format(len(li_cat_to_exclude)))

    # retrieving keywords which target metadatas have to be tagged with
    li_sup_kw = []
    for kw_uuid in environ.get("LIST_TRG_SUP_KEYWORDS_UUID").split(";"):
        retrieved_kw = isogeo.keyword.get(kw_uuid)
        li_sup_kw.append(retrieved_kw)
    logger.info("{} additionnal keywords to tag target with retrieved".format(len(li_sup_kw)))

    # retrieving catalogs that have to be associated to target metadatas
    li_sup_cat = []
    for cat_uuid in environ.get("LIST_TRG_SUP_CAT_UUID").split(";"):
        retrieved_cat = isogeo.catalog.get(
            workgroup_id=environ.get("ISOGEO_ORIGIN_WORKGROUP"),
            catalog_id=cat_uuid
        )
        li_sup_cat.append(retrieved_cat)
    logger.info("{} additional catalogs to associate traget to retrieved".format(len(li_sup_cat)))

    # START TO BROWSE METADATAS UUID FOR MIGRATION
    li_migrated = []
    li_failed = []
    index = 0
    for md in li_uuid_to_migrate:
        logger.info("------- Migrating metadata {}/{} -------".format(index + 1, len(li_uuid_to_migrate)))
        src_uuid = md

        if default_timer() - auth_timer >= 230:
            logger.info("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass
        # loading the metadata to duplicate from his UUID
        try:
            duplicator = MetadataDuplicator(
                api_client=isogeo, source_metadata_uuid=src_uuid
            )
        except Exception as e:
            logger.info("Faile to load {} source metadata : \n {}".format(src_uuid, e))
            li_failed.append(
                [
                    src_uuid,
                ]
            )
            index += 1
            continue

        # change metadata object that gonna be used to update target metadata title to make it specific for AMP
        if "- France Métropolitaine" in duplicator.metadata_source.title:
            duplicator.metadata_source.title = duplicator.metadata_source.title.replace("- France Métropolitaine", "- Métropole Aix-Marseille-Provence")
        elif "- XXXX" in duplicator.metadata_source.title:
            duplicator.metadata_source.title = duplicator.metadata_source.title.replace("- XXXX", "- Métropole Aix-Marseille-Provence")
        else:
            pass
        # remove specifications from metadata object that gonna be used to update target metadata that we don't want to associate target with
        if len(duplicator.metadata_source.specifications):
            duplicator.metadata_source.specifications = []
        else:
            pass

        # store metadata object that gonna be used to update target md
        src_loaded = duplicator.metadata_source

        # check if the metadata exists
        if isinstance(src_loaded, tuple):
            logger.info(
                "{} - There is no accessible source metadata corresponding to this "
                "uuid".format(src_uuid)
            )
            pass

        # import the metadata
        else:
            try:
                md_dst = duplicator.duplicate_into_other_group(
                    destination_workgroup_uuid=environ.get("ISOGEO_ORIGIN_WORKGROUP"),
                    copymark_abstract=False,
                    copymark_title=False,
                    exclude_catalogs=li_cat_to_exclude
                )
                li_migrated.append(
                    [
                        md_dst.name,
                        md_dst._id,
                        src_loaded._id,
                    ]
                )
            except Exception as e:
                logger.info("Failed to import {} into destination workgroup : \n {}".format(src_uuid, e))
                li_failed.append(
                    [
                        src_loaded._id
                    ]
                )
                index += 1
                continue

            # Associate additional subressources to target metadata
            for kw in li_sup_kw:
                isogeo.keyword.tagging(md_dst, kw)

            for cat in li_sup_cat:
                isogeo.catalog.associate_metadata(md_dst, cat)

            index += 1

    isogeo.close()

    csv_result = Path(r"./scripts/AMP/md_ref/rasters/csv/migrated.csv")
    with open(csv_result, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "target_name",
                "target_uuid",
                "source_uuid",
            ]
        )
        for data in li_migrated:
            writer.writerow(data)

    if len(li_failed) > 0:
        logger.info("{} metadatas haven't been migrated. Launch the script again pointing to 'migrate_failed.csv' file".format(len(li_failed)))
        csv_failed = Path(r"./scripts/AMP/md_ref/rasters/csv/migrate_failed.csv")
        with open(csv_failed, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(
                [
                    "source_uuid",
                ]
            )
            for data in li_failed:
                writer.writerow(data)
    else:
        logger.info("All metadatas have been migrated ! :)")