# -*- coding: UTF-8 -*-
#! python3

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
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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


class BackupManager(object):
    """Backup Manager makes it easy to backup Isogeo data (metadata, contacts, workgroups...).
    It uses the Isogeo Python SDK to download data asynchronously.

    :param Isogeo api_client: API client authenticated to Isogeo
    :param str output_folder: path to the folder where to store the exported data
    """

    def __init__(self, api_client: Isogeo, output_folder: str):
        # store API client
        self.isogeo = api_client

        # output folder
        self.outfolder = Path(output_folder)
        if not self.outfolder.exists():
            self.outfolder.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Given output folder doesn't exist. It has been created: {}".format(
                    self.outfolder.resolve()
                )
            )
        if not self.outfolder.is_dir():
            raise TypeError(
                "'output_folder' expect a folder path. Given: {}".format(
                    self.outfolder.resolve()
                )
            )

    def metadata(self, search_params: dict, output_format: str = "json") -> bool:
        """Backups every metadata corresponding at a search.
        It builds a list of metadata to export before transmitting it to an async loop.

        :param dict search params: API client authenticated to Isogeo
        :param str output_format: format of exported data. Until now, only JSON is available.

        :returns: True if export reached the end
        :rtype: bool

        :Example:

        .. code-block:: python

            # prepare backup manager
            backup_mngr = BackupManager(api_client=isogeo, output_folder="./output")

            # build search parameters. For example to filter on two specifics metadata
            search_parameters = {
                "query": None,
                "specific_md": [
                    METADATA_UUID_1,
                    METADATA_UUID_2,
                ],
            }

            # launch the backup
            backup_mngr.metadata(search_params=search_parameters)

        """
        # make the search
        search_to_export = self.isogeo.search(
            # search params
            query=search_params.get("query"),
            page_size=100,
            specific_md=search_params.get("specific_md"),
            # settings
            include="all",
            # whole_results=True
        )

        self.li_api_routes = []
        for i in search_to_export.results:
            # ensure final folder exists
            final_dest = self.outfolder.resolve() / i.get("_creator").get("_id")
            final_dest.mkdir(parents=True, exist_ok=True)

            # build the list of methods to execute
            self.li_api_routes.append(
                {
                    "route": self.isogeo.metadata.get,
                    "params": {"metadata_id": i.get("_id"), "include": "all"},
                    "output_json_name": "{}/{}".format(
                        i.get("_creator").get("_id"), i.get("_id")
                    ),
                }
            )

        # async loop
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._export_metadata_asynchronous())
        loop.run_until_complete(future)

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
    async def _export_metadata_asynchronous(self):
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
        client_id=environ.get("ISOGEO_API_USER_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_CLIENT_SECRET"),
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
