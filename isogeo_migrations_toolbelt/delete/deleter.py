# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

# ------------------------------------------------------------------------------
# Name:         Backup Manager
# Purpose:      Generic module to perform backup from Isogeo
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

    def metadata(self, metadata_ids_list: list) -> bool:
        """Delete every metadata which UUID appears in metadata_ids_list.

        :param list metadata_ids_list: list of Isogeo Metadata UUID to delete

        :returns: True if deletion reached the end
        :rtype: bool

        :Example:

        .. code-block:: python

            # prepare backup manager
            md_dltr = MetadataDeleter(api_client=isogeo)

            # launch the backup
            md_dltr.metadata(metadata_ids_list=li_uuid)

        """

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

        # prepare the list of request to Isogeo API
        self.li_api_routes = []
        for uuid in li_uuid:
            # build the list of methods to execute
            self.li_api_routes.append(
                {
                    "route": self.isogeo.metadata.delete,
                    "params": {"metadata_id": uuid}
                }
            )

        # async loop
        if self.loop.is_closed():
            logger.debug(
                "Current event loop is already closed. Creating a new one..."
            )
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        task = self.loop.create_task(self._delete_metadata_asynchronous())
        self.loop.run_until_complete(task)
        return True

    def _store_to_json(self, func_outname_params: dict):
        """Meta function meant to be executed in async mode.
        In charge to make the request to the Isogeo API and store the result into a JSON file.

        :param dict func_outname_params: parameters for the execution. Expected structure:

            .. code-block:: python

                {
                    "route": self.isogeo.metadata.get,
                    "params": {"metadata_id": METADATA_UUID, "include": "all"},
                    "output_json_name": "{}/{}".format(WORKGROUP_UUID, METADATA_UUID),
                }

        """
        route_method = func_outname_params.get("route")
        out_filename = Path(
            self.outfolder.resolve(),
            func_outname_params.get("output_json_name") + ".json",
        )

        try:
            # use request
            request = route_method(**func_outname_params.get("params"))
            # transform objects into dicts
            if not isinstance(request, (dict, list)):
                request = request.to_dict()
            # store response into a json file
            with out_filename.open("w") as out_json:
                json.dump(
                    obj=request, fp=out_json, sort_keys=True, indent=4, default=str
                )
        except Exception as e:
            logger.error(
                "Export failed to '{output_json_name}.json' "
                "using route '{route}' "
                "with these params '{params}'".format(**func_outname_params)
            )
            logger.error(e)

    # -- ASYNC METHODS -----------------------------------------------------------------
    async def _delete_metadata_asynchronous(self):
        """Async loop builder."""
        with ThreadPoolExecutor(
            max_workers=5, thread_name_prefix="IsogeoBackupManager_"
        ) as executor:
            # Set any session parameters here before calling `fetch`
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    self._store_to_json,
                    # Allows us to pass in multiple arguments to `fetch`
                    *(api_route,),
                )
                for api_route in self.li_api_routes
            ]

            # store responses
            out_list = []
            for response in await asyncio.gather(*tasks):
                out_list.append(response)

            return out_list


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
    load_dotenv("prod.env", override=True)

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
