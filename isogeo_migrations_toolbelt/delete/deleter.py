# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

# ------------------------------------------------------------------------------
# Name:         Metadata Deleter
# Purpose:      Generic module to perform Isogeo metadata deletion
# Author:       Isogeo
#
# Python:       3.6+
# ------------------------------------------------------------------------------

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

# 3rd party
import urllib3

# Isogeo
from isogeo_pysdk import Isogeo
from isogeo_pysdk.checker import IsogeoChecker

# #############################################################################
# ######## Globals #################
# ##################################

# logs
logger = logging.getLogger(__name__)
checker = IsogeoChecker()

# ############################################################################
# ########## Classes #############
# ################################


class MetadataDeleter(object):
    """Backup Manager makes it easy to backup Isogeo data (metadata, contacts, workgroups...).
    It uses the Isogeo Python SDK to download data asynchronously.

    :param Isogeo api_client: API client authenticated to Isogeo
    :param str output_folder: path to the folder where to store the exported data
    """

    def __init__(self, api_client: Isogeo):
        # store API client
        self.isogeo = api_client

        try:
            self.loop = asyncio.get_event_loop()
        except Exception as e:
            logger.debug("An error occured instanciating the loop : {}".format(e))
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.nb_deleted = 0
        self.nb_to_delete = 0
        self.deleted = []

        self.hard_mode = 0

    def delete(self, metadata_ids_list: list, hard_mode: bool=0) -> bool:
        """Delete every metadata which UUID appears in metadata_ids_list.

        :param list metadata_ids_list: list of Isogeo Metadata UUID to delete
        :param bool hard_mode:

        :Example:

        .. code-block:: python

            # prepare deletion manager
            md_dltr = MetadataDeleter(api_client=isogeo)

            # launch the deletion
            md_dltr.delete(metadata_ids_list=li_uuid, hard_mode=1)

        """
        self.hard_mode = hard_mode

        # check the UUID list content validity:
        li_uuid = []
        for uuid in metadata_ids_list:
            if checker.check_is_uuid(uuid):
                li_uuid.append(uuid)
            else:
                logger.info("{} is not a valid UUID.".format(uuid))
        nb_invalid_uuid = len(metadata_ids_list) - len(li_uuid)

        # debrief to the user
        if nb_invalid_uuid == 0:
            logger.info("All UUID from the list passed the check.")
        else:
            logger.info("{} UUIDs didn't pass the check.".format(nb_invalid_uuid))
        self.nb_to_delete = len(li_uuid)

        # prepare the list of request to Isogeo API
        self.li_api_routes = []
        for uuid in li_uuid:
            # build the list of methods to execute
            self.li_api_routes.append(
                {"route": self.isogeo.metadata.delete, "params": {"metadata_id": uuid}}
            )

        # async loop
        if self.loop.is_closed():
            logger.debug("Current event loop is already closed. Creating a new one...")
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Inform the user
        if self.hard_mode:
            logger.warning("HARD MODE ACTIVATED >>> {} metadatas gonna be deleted".format(self.nb_to_delete))
        else:
            pass

        # launch the task
        task = self.loop.create_task(self._delete_metadata_asynchronous())
        self.loop.run_until_complete(task)

        logger.info("{}/{} metadatas have been deleted".format(self.nb_deleted, self.nb_to_delete))

    def _delete_metadata(self, func_outname_params: dict):
        """Meta function meant to be executed in async mode.
        In charge to make the deletion request to the Isogeo API.

        :param dict func_outname_params: parameters for the execution. Expected structure:

            .. code-block:: python

                {
                    "route": self.isogeo.metadata.delete,
                    "params": {"metadata_id": METADATA_UUID}
                }

        """
        # retrieve Isogeo API route to call
        route_method = func_outname_params.get("route")

        try:
            # use request
            if self.hard_mode:
                request = route_method(**func_outname_params.get("params"))
            else:
                request = "soft"
                pass
            self.nb_deleted += 1
            return request
        except Exception as e:
            logger.error(
                "Deletion failed using route '{route}' with these params '{params}'".format(
                    **func_outname_params
                )
            )
            logger.error(e)
            return "error"

    # -- ASYNC METHODS -----------------------------------------------------------------
    async def _delete_metadata_asynchronous(self):
        """Async loop builder."""
        with ThreadPoolExecutor(
            max_workers=5, thread_name_prefix="IsogeoMetadataDeleter_"
        ) as executor:
            # Set any session parameters here before calling `fetch`
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    self._delete_metadata,
                    # Allows us to pass in multiple arguments to `fetch`
                    *(api_route,),
                )
                for api_route in self.li_api_routes
            ]

            # store responses
            for response in await asyncio.gather(*tasks):
                self.deleted.append(response)


# #############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    """Standalone execution for quick and dirty use or test"""
    # additional imports
    from logging.handlers import RotatingFileHandler
    from os import environ

    # 3rd party
    from dotenv import load_dotenv

    # ------------ Log & debug ----------------
    logger = logging.getLogger()
    logging.captureWarnings(True)
    logger.setLevel(logging.DEBUG)
    # logger.setLevel(logging.INFO)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler("dev_debug.log", "a", 3000000, 1)
    log_file_handler.setLevel(logging.DEBUG)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # environment vars
    load_dotenv("./env/.env", override=True)

    # ignore warnings related to the QA self-signed cert
    if environ.get("ISOGEO_PLATFORM").lower() == "qa":
        urllib3.disable_warnings()

    # establish isogeo connection
    isogeo = Isogeo(
        client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
        auth_mode="user_legacy",
    )

    # getting a token
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )

    # close connection
    isogeo.close()
