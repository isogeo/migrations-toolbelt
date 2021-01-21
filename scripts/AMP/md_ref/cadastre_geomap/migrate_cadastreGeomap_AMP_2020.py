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
import csv
import logging
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
    logger.info("-------------- RETRIEVING INFOS FROM MAPPING TABLE ------------------")

    # ################# CHECK MAPPING TABLE and RETRIEVE UUID FROM IT #################
    # to store source metadata uuid, title and name that passe the tests
    li_src_to_migrate = []
    # store all source uuid that appear in the mapping table
    src_found = []
    # to store target metadata uuid, title and name that passe the tests
    li_trg_to_migrate = []
    # store all target uuid that appear in the mapping table
    trg_found = []
    # store all source and target metadata uuid
    li_to_backup = []
    # prepare csv reading
    input_csv = Path(r"./scripts/AMP/md_ref/cadastre_geomap/csv/matching_cadastre_migration.csv")
    fieldnames = [
        "target_name",
        "target_uuid",
        "source_uuid",
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter="|", fieldnames=fieldnames)

        row_num = 0
        for row in reader:
            row_num += 1
            trg_name = row.get("target_name")
            trg_uuid = row.get("target_uuid")
            src_uuid = row.get("source_uuid")
            if src_uuid != "source_uuid":  # PROD
                src_found.append(src_uuid)
                trg_found.append(trg_uuid)
                # check if the target metadata exists
                if trg_uuid == "NR":
                    logger.info("l.{} - there is no target".format(row_num))
                # check source UUID validity
                elif not checker.check_is_uuid(src_uuid):
                    logger.info(
                        "l.{} - {} source UUID isn't valid".format(row_num, src_uuid)
                    )
                # check if source UUID appears just one time in the field
                elif li_src_to_migrate.count(src_uuid) > 0:
                    logger.info(
                        "l.{} - {} already exist in the tab at line {}".format(
                            row_num, src_uuid, str(src_found.index(src_uuid) + 1)
                        )
                    )
                # if UUID, title and name of source metadata have passed all checks,
                # time to test UUID and nam of target metadata
                else:
                    # check target UUID validity
                    if not checker.check_is_uuid(trg_uuid):
                        logger.info(
                            "l.{} -{} target UUID isn't valid".format(row_num, trg_uuid)
                        )
                    # check if target UUID appears just one time in the field
                    elif li_trg_to_migrate.count(trg_uuid) > 0:
                        logger.info(
                            "l.{} - {} target UUID already exist in the tab at line {}".format(
                                row_num, trg_uuid, str(trg_found.index(trg_uuid) + 1)
                            )
                        )
                    # check if target UUID is different from source UUID
                    elif trg_uuid == src_uuid:
                        logger.info(
                            "l.{} - {} target and source UUID are the same".format(
                                row_num, trg_uuid
                            )
                        )
                    # if all check are passed, metadata are stored into a tuple that is
                    # added to a list
                    else:
                        to_migrate = (src_uuid)
                        li_src_to_migrate.append(to_migrate)
                        to_migrate = (trg_uuid, trg_name)
                        li_trg_to_migrate.append(to_migrate)

                        li_to_backup.append(src_uuid)
                        li_to_backup.append(trg_uuid)
            else:
                pass

    # once each row have been test, a summary of the checks is logged
    expected_uuid_nb = len(src_found)
    found_uuid_nbr = len(li_src_to_migrate)
    if found_uuid_nbr == expected_uuid_nb:
        logger.info("--> All lines passed the check.")
    else:
        logger.info(
            "--> {}/{} lines didn't passe the check.".format(
                expected_uuid_nb - found_uuid_nbr, expected_uuid_nb
            )
        )
    if len(set(li_src_to_migrate)) == found_uuid_nbr:
        logger.info("--> Each source uuid appears only once.")
    else:
        logger.info("--> There is some duplicate source uuid.")

    found_uuid_nbr = len(li_trg_to_migrate)
    if len(set(li_trg_to_migrate)) == found_uuid_nbr:
        logger.info("--> Each target uuid appears only once.")
    else:
        logger.info("--> There is some duplicate target uuid.")

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

    logger.info(
        "{} metadatas will be migrated".format(
            len(li_src_to_migrate)
        )
    )

    # ------------------------------------ BACKUP --------------------------------------
    if int(environ.get("BACKUP")) == 1 and len(li_to_backup) > 0:
        logger.info("---------------------------- BACKUP {} metadatas---------------------------------".format(len(li_to_backup)))
        # backup manager instanciation
        backup_path = Path(r"./scripts/AMP/md_ref/cadastre_geomap/_output/_backup")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50

        if len(li_to_backup) > amplitude:
            bound_range = int(len(li_to_backup) / amplitude)
            li_bound = []
            for i in range(bound_range + 1):
                li_bound.append(amplitude * i)
            li_bound.append(len(li_to_backup))

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

                search_parameters = {"query": None, "specific_md": tuple(li_to_backup[bound_inf:bound_sup])}
                try:
                    backup_mng.metadata(search_params=search_parameters)
                except Exception as e:
                    logger.info("an error occured : {}".format(e))
        else:
            search_parameters = {"query": None, "specific_md": tuple(li_to_backup)}
            backup_mng.metadata(search_params=search_parameters)
    else:
        pass

    # ----------------------------------- MIGRATING ------------------------------------
    logger.info("--------------------------- MIGRATING {} metadatas -------------------------------".format(len(li_src_to_migrate)))

    # RETRIEVING SOME SUBRESSOURCES WE WANT OR DON'T WANT TO ASSOCIATE TARGET METADATAS WITH 
    # retrieve source workgroup catalogs we don't want to associate target metadatas with
    li_cat_to_exclude = []
    cat_search = isogeo.catalog.listing(workgroup_id=environ.get("ISOGEO_DGFIP_WORKGROUP"))
    for cat in cat_search:
        li_cat_to_exclude.append(cat.get("_id"))

    # retrieving keywords which target metadatas have to be tagged with
    li_sup_kw = []
    for kw_uuid in environ.get("LIST_TRG_SUP_KEYWORDS_UUID").split(";"):
        retrieved_kw = isogeo.keyword.get(kw_uuid)
        li_sup_kw.append(retrieved_kw)

    # retrieving catalogs that have to be associated to target metadatas
    li_sup_cat = []
    for cat_uuid in environ.get("LIST_TRG_SUP_CAT_UUID").split(";"):
        retrieved_cat = isogeo.catalog.get(
            workgroup_id=environ.get("ISOGEO_ORIGIN_WORKGROUP"),
            catalog_id=cat_uuid
        )
        li_sup_cat.append(retrieved_cat)

    # START TO BROWSE METADATAS UUID FOR MIGRATION
    li_migrated = []
    li_failed = []
    index = 0
    for md in li_src_to_migrate:
        logger.info("------- Migrating metadata {}/{} -------".format(index + 1, len(li_src_to_migrate)))
        src_uuid = md
        trg_uuid = li_trg_to_migrate[index][0]
        trg_name = li_trg_to_migrate[index][1]

        if default_timer() - auth_timer >= 6900:
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
            src_migrator = MetadataDuplicator(
                api_client=isogeo, source_metadata_uuid=src_uuid
            )
        except Exception as e:
            logger.info("Faile to load {} source metadata : \n {}".format(src_uuid, e))
            li_failed.append(
                [
                    src_uuid,
                    trg_name,
                    trg_uuid
                ]
            )
            index += 1
            continue

        # change metadata object that gonna be used to update target metadata title to make it specific for AMP
        src_migrator.metadata_source.title = src_migrator.metadata_source.title.replace(" - XXXX", " - Métropole Aix-Marseille Provence")

        # remove subressources from metadata object that gonna be used to update target metadata that we don't want to associate target with
        # remove events
        if len(src_migrator.metadata_source.events):
            src_migrator.metadata_source.events = []
        else:
            pass
        # remove specifications
        if len(src_migrator.metadata_source.specifications):
            src_migrator.metadata_source.specifications = []
        else:
            pass

        # store metadata object that gonna be used to update target md
        src_loaded = src_migrator.metadata_source

        # check if the metadata exists
        if isinstance(src_loaded, tuple):
            logger.info(
                "{} - There is no accessible source metadata corresponding to this "
                "uuid".format(src_uuid)
            )
            pass

        # migrate the metadata
        else:
            li_exclude_fields = [
                "coordinateSystem",
                "envelope",
                "features",
                "geometry",
                "name",
                "path",
                "format",
                "formatVersion",
                "series",
            ]
            try:
                md_dst = src_migrator.import_into_other_metadata(
                    copymark_abstract=False,  # FALSE EN PROD
                    copymark_title=False,  # FALSE EN PROD
                    destination_metadata_uuid=trg_uuid,
                    exclude_fields=li_exclude_fields,
                    exclude_catalogs=li_cat_to_exclude,
                    switch_service_layers=True
                )
                li_migrated.append(
                    [
                        md_dst.name,
                        md_dst._id,
                        src_loaded._id,
                    ]
                )
            except Exception as e:
                logger.info("Failed to import {} into {} : \n {}".format(src_uuid, trg_uuid, e))
                li_failed.append(
                    [
                        trg_name,
                        trg_uuid,
                        src_uuid,
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

    csv_result = Path(r"./scripts/AMP/md_ref/cadastre_geomap/csv/migrated.csv")
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
        csv_failed = Path(r"./scripts/AMP/md_ref/cadastre_geomap/csv/migrate_failed.csv")
        with open(csv_failed, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(
                [
                    "target_name",
                    "target_uuid",
                    "source_uuid",
                ]
            )
            for data in li_failed:
                writer.writerow(data)
    else:
        logger.info("All metadatas have been migrated ! :)")