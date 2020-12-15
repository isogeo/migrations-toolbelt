# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to update matching table for Belfort migration
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
from os import environ
from pathlib import Path

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/belfort.env", override=True)

if __name__ == "__main__":
    # shortcuts
    origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    src_cat_uuid = environ.get("ISOGEO_CATALOG_SOURCE")
    trg_cat_uuid = environ.get("ISOGEO_CATALOG_TARGET")
    li_excluded_src = [
        "ed240fbab48d49b39e69583479a69df6", "5441a06c5cf8420fa5f2190a14f83333", "c18335bcdd4947538d607c2a5dff34e0", "e83c415743d2452193e37b5b3506fcb4", "505d905693ea45eeb1a0b64a0faae7e0", "a009f133665d469fb174b7d3a2fe9680", "a9fd9fefa6234bc9917f0e4a644c587b", "e24b8725206045d9b35ffb2686bd87fc", "42a17d45718e4cd2943a9b7ded977381", "264943fbb99a4835ae820363612b9632", "b81cef14b9cc428d9d61d79f6821c571", "521582a1803a4e149fb6619147bda22c", "3f231cfe6ded4e928abc862ba9e960df", "af56ce63d23e45f5a7a011d7b2f5289a", "53582946eadd4a15833ff0dc14e40d5d", "b26868d8cca441c7be304a8f87cea65a", "614c7fb5911143f68e3e24777041d25d", "f568a9d81a724018b1f70e72be9deb63", "e6569b29d84043efb0536bcd63e6970c", "c3968adf86da4db887c32e02db03e307", "a2a1a6c2bfcc4cc8904f7809c0c30bbb", "072789accdca42a48a741cc76573d201", "12fe5cd490a540e1a63e9f4fdded0ba9", "4ad4a1cdae274b849882a035d2a3a84f", "4dcc63e1caf14f4f97d9a596770385ef", "443cce4a39fa4ac391f0f91917ade713", "2e78f7cb19c84dc08f4aa5619c32e063", "c382a885da0044e8a9f962ff270ee51b", "7aff2b4132ce4a0b9bfa96d9f7bba67c", "dd7a580aa31c4626bc0ea757b047e9f9", "d3e6f5babddd4adc8280d01d0a39c5bb", "0ede1093a90a4c7d9f10016e376ee4f8", "193b95b54c2e4484bfd9f7a53f4789e2", "bad91a05ef21416eb5862c02a45145c7", "29795fa9056f4d73b2a90b0ded9feb62", "f1d6a42460ac4a7cbbda57b888aeb824", "67c4f91f71c94b0c995fb33b7b10c65d", "9000504067b646b9a84cd38abc066a8e", "c7cd26a0ef994d10b07896c4d114a14a", "d2f6e2bc30d5483ab471a3677bd5a5f6", "731f2967e4e04be9bd6401ac4d6d3099", "214df67e594a4914b9a807eccf65f6e5", "d890ffdfb6a44680be15bdd8a76f9dfc", "0d38fca86f9f4853afce75a7ae21f329", "4322178b79044f6f9c1db31c1aa66bd9", "8a1401f05720432cbf1d0d9a3619bb00", "2d0df5e7f2a94d939968520ea4b4ae71", "a5b915cd64d94bb6a634ed8b5d5f84a9", "70f7aa656c524a82917ef82cfafbb459", "2f4685cf73e943d8b11d438e2e665199", "00d4d769f3f840daaf56bb70c420f005", 
    ]
    # ############################### LOADING SOURCE AND TARGET METADATAS INFOS ###############################
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

    # request Isogeo API about source metadatas
    whole_search = isogeo.search(
        group=origin_wg_uuid,
        whole_results=True
    )

    isogeo.close()

    # retrieve source metadatas infos from Isogeo API response
    li_md_infos = []
    for md in whole_search.results:
        li_md_infos.append((md.get("_id"), md.get("title"), md.get("name", "NR")))

    # ############################### LOADING ORIGINAL MATCHING TABLE ###############################
    input_csv = Path(r"./scripts/belfort/csv/correspondances_light.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
        "match_type"
    ]

    # retrieve matching from csv file
    li_from_csv = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            if reader.line_num > 1 and row.get("source_uuid") not in li_excluded_src:
                li_from_csv.append(
                    [
                        row.get("source_uuid"),
                        row.get("source_title"),
                        row.get("source_name"),
                        row.get("target_name"),
                        row.get("target_uuid"),
                        row.get("match_type"),
                    ]
                )
            else:
                pass

    li_for_csv = []

    for line in li_from_csv:
        if line[4] == "NR":
            li_trg_info = [info for info in li_md_infos if info[2] == line[3]]
            if len(li_trg_info) == 1:
                line[4] = li_trg_info[0][0]
            elif len(li_trg_info) == 0:
                line[4] = "uuid_not_found"
            else:
                line[4] = ";".join([info[0] for info in li_trg_info])
        else:
            pass
        li_for_csv.append(line)

    csv_path = Path(r"./scripts/belfort/csv/correspondances.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "match_type"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
