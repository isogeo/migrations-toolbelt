# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Matching table creation script to proceed the migration of BD TOPO 3 for AMP
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
from isogeo_pysdk import Isogeo

# load .env file
load_dotenv("./env/mamp.env", override=True)

if __name__ == "__main__":

    # ############################### MIGRATING & SAVING ###############################
    amp_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    target_cat_uuid = environ.get("AMP_BDTOPO3_CATALOG_UUID")

    ign_wg_uuid = environ.get("ISOGEO_IGN_WORKGROUP")
    source_cat_uuid = environ.get("IGN_BDTOPO3_CADALOG_UUID")

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
    # load source metadatas' uuid, title and name
    src_md_search = isogeo.search(
        group=ign_wg_uuid,
        query="catalog:{}".format(source_cat_uuid),
        whole_results=True
    )
    li_md_src = []
    li_name_src = []
    for md in src_md_search.results:
        li_md_src.append((md.get("_id"), md.get("title"), md.get("name")))
        li_name_src.append(md.get("name"))

    # load target metadatas' uuid, title and name
    trg_md_search = isogeo.search(
        group=amp_wg_uuid,
        query="catalog:{}".format(target_cat_uuid),
        whole_results=True
    )
    li_md_trg = []
    for md in trg_md_search.results:
        li_md_trg.append((md.get("_id"), md.get("title"), md.get("name")))

    li_for_csv = []
    nb_matched = 0
    for md_trg in li_md_trg:
        md_trg_short_name = md_trg[2].replace("ref93.REF_BDT_V3_", "")
        if md_trg_short_name in li_name_src:
            index_src = li_name_src.index(md_trg_short_name)
            md_src = li_md_src[index_src]

            li_for_csv.append(
                [
                    md_trg[0],
                    md_trg[1],
                    md_trg[2],
                    md_src[2],
                    md_src[0],
                    md_src[1],
                    "name_rule"
                ]
            )
            nb_matched += 1
        else:
            li_for_csv.append(
                [
                    md_trg[0],
                    md_trg[1],
                    md_trg[2],
                    "NR",
                    "NR",
                    "NR",
                    "no_match"
                ]
            )
            pass

    # li_no_match = [line for line in li_for_csv if line[6] == "no_match"]
    # for line in li_no_match:
    #     trg_title = line[1]
    #     line_index_csv = li_for_csv.index(line)
    #     for md_src in li_md_src:
    #         src_short_title = md_src[1].split(" [BD TOPO®] ")[0]
    #         if src_short_title in trg_title:
    #             li_for_csv[line_index_csv][3] = md_src[2]
    #             li_for_csv[line_index_csv][4] = md_src[0]
    #             li_for_csv[line_index_csv][5] = md_src[1]
    #             li_for_csv[line_index_csv][6] = "title_rule"
    #             nb_matched += 1
    #         else:
    #             pass

    print("{} on {} source metadata have matched with a target".format(nb_matched, len(li_for_csv)))

    csv_path = Path(r"./scripts/AMP/md_ref/bd_topo_3/csv/matching_bd_topo_3.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "target_uuid",
                "target_title",
                "target_name",
                "source_name",
                "source_uuid",
                "source_title",
                "match_type"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
