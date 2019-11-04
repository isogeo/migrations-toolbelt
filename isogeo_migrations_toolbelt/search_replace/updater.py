# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

"""
    Name:         Backup Manager
    Purpose:      Generic module to perform backup from Isogeo
    Author:       Isogeo

    Python:       3.6+
"""

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import asyncio
import logging

# Isogeo
from isogeo_pysdk import Isogeo, Metadata

# #############################################################################
# ######## Globals #################
# ##################################

# logs
logger = logging.getLogger(__name__)


# ############################################################################
# ########## Classes #############
# ################################
class MetadataUpdater:
    def __init__(
        self, api_client: Isogeo, metadatas_ready_to_be_updated: list, max_workers=10
    ):
        # store API client
        self.isogeo = api_client

        # store list of metadatas
        self.in_metadatas = metadatas_ready_to_be_updated

        # create a queue that only allows a maximum of two items
        self.queue_updating = asyncio.Queue()
        self.max_workers = max_workers

    async def batch_updates(self):
        # DON'T await here; start consuming things out of the queue, and
        # meanwhile execution of this function continues. We'll start two
        # coroutines for fetching and two coroutines for processing.
        all_the_coros = asyncio.gather(
            *[self._worker(i) for i in range(self.max_workers)]
        )

        # place all URLs on the queue
        for metadata in self.in_metadatas:
            await self.queue_updating.put(metadata)

        # now put a bunch of `None`'s in the queue as signals to the workers
        # that there are no more items in the queue.
        for _ in range(self.max_workers):
            await self.queue_updating.put(None)

        # now make sure everything is done
        await all_the_coros

    async def _worker(self, i):
        while True:
            metadata = await self.queue_updating.get()
            if metadata is None:
                # this coroutine is done; simply return to exit
                return

            logger.debug(f"Fetch worker {i} is updating the metadata: {metadata._id}")
            await self.update(metadata)

    async def update(self, metadata: Metadata):
        logger.debug("Updating metadata: " + metadata.title_or_name())
        md_updated = self.isogeo.metadata.update(metadata)
        # await asyncio.sleep(2)
        if isinstance(md_updated, Metadata):
            logger.debug(f"{metadata._id} has been updated")
        elif isinstance(md_updated, tuple):
            logger.error(f"{metadata._id} can't be updated: {md_updated[1]}")


# #############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    """Standalone execution for quick and dirty use or test"""
    # standard
    from os import environ

    # 3rd party
    from dotenv import load_dotenv
    import urllib3

    # ------------ Log & debug ----------------
    logger = logging.getLogger()
    logging.captureWarnings(True)
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_console_handler)

    # environment vars
    load_dotenv("dev.env", override=True)

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

    # sample search
    metadatas_to_update = isogeo.search(
        group=environ.get("ISOGEO_WORKGROUP_TEST_UUID"), whole_results=1
    )
    logger.info("{} metadatas to update".format(len(metadatas_to_update.results)))

    # make a little update
    li_ready_to_be_updated = []
    for md in metadatas_to_update.results:
        metadata = Metadata.clean_attributes(md)
        if "\n\n MIGRATIONS TOOLBELT" in metadata.abstract:
            metadata.abstract = metadata.abstract.replace(
                "\n\n MIGRATIONS TOOLBELT", ""
            )
        elif metadata.abstract:
            metadata.abstract += "\n\n MIGRATIONS TOOLBELT"
        elif not metadata.abstract:
            metadata.abstract = "\n\n MIGRATIONS TOOLBELT"
        else:
            continue

        li_ready_to_be_updated.append(metadata)

    # additional imports
    updater = MetadataUpdater(
        api_client=isogeo, metadatas_ready_to_be_updated=li_ready_to_be_updated
    )
    asyncio.run(updater.batch_updates())

    isogeo.close()
