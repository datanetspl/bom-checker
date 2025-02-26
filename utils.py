###import all the libraries
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import io
import warnings

warnings.simplefilter(action="ignore")
import pandas as pd
from datetime import datetime, timezone
import numpy as np
import re
import glob
import os
import pickle
import codecs
import subprocess
import webbrowser
import shutil
import time
from natsort import index_natsorted  # for version sorting of hierarchical numbers
#import xlwings as xw
import math
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import requests, msal
import json
from typing import Dict
import streamlit as st
verbose = True

# import credentials
#with open("./secrets.pkl", "rb") as f:
#    secrets = pickle.load(f)
#secrets = {"username":"", "password":"", "tenant_id":"", "client_id":"", "client_secret":"", "environment":""}
username = st.secrets["username"]
password = st.secrets["password"]
tenant_id = st.secrets["tenant_id"]
client_id = st.secrets["client_id"]
client_secret = st.secrets["client_secret"]
environment = st.secrets["environment"]


expendables_list_path = r"https://akribissg.sharepoint.com/sites/AkribisSGCTBPortal/Shared%20Documents/Stage%20Non-Inventorized%20Expendables%20List.xlsx"
output_sharepoint_site_url = "https://akribissg.sharepoint.com/sites/AkribisSGCTBPortal/"



def getToken(tenant, client_id, client_secret):
    authority = "https://login.microsoftonline.com/" + tenant
    scope = ["https://api.businesscentral.dynamics.com/.default"]

    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)

    try:
        accessToken = app.acquire_token_for_client(scopes=scope)
        if accessToken["access_token"]:
            print("New access token retreived....")
        else:
            print("Error aquiring authorization token.")
    except Exception as err:
        print(err)
        raise Exception

    return accessToken


def retrieve_all_records(session, request_url, request_header):
    all_records = []
    url = request_url
    i = 1
    while True:
        if not url:
            break
        print("Getting page %d." % i)
        response = session.get(url, headers=request_header)
        if response.status_code == 200:
            json_data = json.loads(response.text)
            all_records = all_records + json_data["value"]
            if "@odata.nextLink" in json_data.keys():
                url = json_data["@odata.nextLink"]
            else:
                url = None
        else:
            raise ValueError("Status Code %s" % response.status_code)
        i += 1

    return all_records

@st.cache_resource
def get_expendables_list():
    expendables_list_path = r"https://akribissg.sharepoint.com/sites/AkribisSGCTBPortal/Shared%20Documents/Stage%20Non-Inventorized%20Expendables%20List.xlsx"
    expendables_url = r"https://akribissg.sharepoint.com/sites/AkribisSGCTBPortal"
    ctx = login_to_sharepoint(expendables_url, username, password, verbose=False)
    expendables_list_relative_path = expendables_list_path.replace(output_sharepoint_site_url, "")

    # download expendables_list to local file
    filename = "expendables_list.xlsx"
    with open(filename, "wb") as output_file:
        if verbose:
            print("Downloading Expendables List...")
        file = ctx.web.get_file_by_server_relative_url(expendables_list_relative_path).download(output_file).execute_query()
        if verbose:
            print("Expendables List downloaded.")

    # read expendables list into DataFrame
    with open(filename, "rb") as input_file:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            expendables_df = pd.read_excel(
                input_file, sheet_name="Sheet1", engine="openpyxl", skiprows=0, usecols="A:D", converters={"Description": str, "Description 2": str, "Manufacturer": str, "Status": str}
            )

    expendables_df = expendables_df[expendables_df["Status"] == "Released"]
    expendables_df["Description"] = expendables_df["Description"].str.strip()
    expendables_df["Description"] = expendables_df["Description"].str.upper()

    os.remove(filename)  # delete local copy of expendables list after reading into memory
    return expendables_df


def login_to_sharepoint(url, username, password, verbose=False):
    ctx_auth = AuthenticationContext(url)
    if verbose:
        print("Authenticating...")
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        if verbose:
            print("Authenticated.")
    return ctx


def get_raw_bom(bom_path, ctx, url, verbose=False):
    bom_relative_path = bom_path.replace(url, "")
    bom_filename = bom_relative_path.split("/")[-1].replace("%20", " ")

    # download bom to local file
    filename = "bom.xlsx"
    with open(filename, "wb") as output_file:
        if verbose:
            print("Downloading BOM...")
        file = ctx.web.get_file_by_server_relative_url(bom_relative_path).download(output_file).execute_query()
        if verbose:
            print("BOM downloaded.")
    # read system and contingency bom files to dataframes and flatten
    with open(filename, "rb") as input_file:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            system_bom_df = pd.read_excel(
                input_file,
                sheet_name="SYSTEM BOM",
                engine="openpyxl",
                skiprows=11,
                usecols="A:L",
                converters={
                    "Hierarchical No.": str,
                    "System No.": str,
                    "Description\n(Order Part No / Dwg No / REV No.)": str,
                    "Description 2\n(Description / Dwg Title)": str,
                    "Qty": float,
                    "UOM": str,
                    "Unit Cost [SGD]": float,
                    "Total Cost [SGD]": float,
                    "Manufacturer": str,
                    "Drawing reference": str,
                    "WIP or Released": str,
                    "Obsolete": str,
                },
            )
            if "Obselete" in set(system_bom_df.keys()):
                system_bom_df = system_bom_df.rename(columns={"Obselete": "Obsolete"})
            system_bom_df = system_bom_df.dropna(axis=0, subset=["Hierarchical No."]).reset_index(drop=True)
            if len(system_bom_df) > 0:
                for key in ["Hierarchical No.", "System No.", "UOM", "Manufacturer", "Obsolete"]:
                    system_bom_df[key] = system_bom_df[key].str.strip()
                    system_bom_df[key] = system_bom_df[key].str.upper()
            contingency_bom_df = pd.read_excel(
                input_file,
                sheet_name="CONTINGENCY BOM",
                engine="openpyxl",
                usecols="A:L",
                skiprows=11,
                converters={
                    "Hierarchical No.": str,
                    "System No.": str,
                    "Description\n(Order Part No / Dwg No / REV No.)": str,
                    "Description 2\n(Description / Dwg Title)": str,
                    "Qty": float,
                    "UOM": str,
                    "Unit Cost [SGD]": float,
                    "Total Cost [SGD]": float,
                    "Manufacturer": str,
                    "Drawing reference": str,
                    "WIP or Released": str,
                    "Obsolete": str,
                },
            )
            if "Obselete" in set(contingency_bom_df.keys()):
                contingency_bom_df = contingency_bom_df.rename(columns={"Obselete": "Obsolete"})
            contingency_bom_df = contingency_bom_df.dropna(axis=0, subset=["Hierarchical No."]).reset_index(drop=True)
            if len(contingency_bom_df) > 0:
                for key in ["Hierarchical No.", "System No.", "UOM", "Manufacturer", "Obsolete"]:
                    contingency_bom_df[key] = contingency_bom_df[key].str.strip()

    os.remove(filename)  # delete local copy of BOM after reading into memory
    return (system_bom_df, contingency_bom_df, bom_filename)


def handle_integer_hierarchical_numbers(s):
    s = str(s)

    split = s.split(".")
    if len(split) == 2 and split[-1] == "0":
        return split[0]
    else:
        return s

@st.cache_resource
def get_BC_data_api_generic(tenant_id, client_id, client_secret, environment):
    # define the retry strategy
    retry_strategy = Retry(
        total=4,  # maximum number of retries
        status_forcelist=[429, 500, 502, 503, 504],  # the HTTP status codes to retry on
    )

    # create an HTTP adapter with the retry strategy and mount it to the session
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # create a new session object
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Fetch the token as json object
    reqToken = getToken(tenant_id, client_id, client_secret)

    # Build the request Headers
    reqHeaders = {"Accept-Language": "en-us", "Authorization": f"Bearer {reqToken['access_token']}", "Prefer": "odata.maxpagesize=10000"}

    # Fetch and parse item_master_df
    print("Retrieving item master list...")
    t_start = time.time()
    request_url = f"https://api.businesscentral.dynamics.com/v2.0/{tenant_id}/{environment}/api/v2.0/items?company=Akribis%20Systems%20Pte%20Ltd&$select=number,displayName,baseUnitOfMeasureCode,unitCost,type&$filter=type eq 'Inventory'"
    response = retrieve_all_records(session, request_url, reqHeaders)
    item_master_df = pd.DataFrame(response)
    item_master_df = item_master_df.rename(columns={"number": "System No.", "displayName": "Description", "baseUnitOfMeasureCode": "UOM", "unitCost": "Unit Cost"})
    item_master_df["UOM"] = item_master_df["UOM"].astype("category")
    item_master_df["Unit Cost"] = item_master_df["Unit Cost"].astype("float16")
    item_master_df["Description"] = item_master_df["Description"].str.strip()
    item_master_df["Description"] = item_master_df["Description"].str.upper()
    item_master_df = item_master_df[["System No.", "Description", "UOM", "Unit Cost"]]
    item_master_df.reset_index(drop=True)
    t_elapsed = time.time() - t_start
    print("Item master list retrieved. It took %d seconds." % t_elapsed)

    # Fetch and parse secondary_uom_df
    t_start = time.time()
    print("Retrieving items UOM conversion table...")
    request_url = f"https://api.businesscentral.dynamics.com/v2.0/3a0b8048-1167-4662-ba9d-a0247c4e0ca6/Production/api/BC/Integration/v2.0/companies(4a2d30ee-aa46-eb11-bb27-00224857e939)/itemUnitOfMeasure?$select=itemNo,code,qtyPerUnitOfMeasure"
    response = retrieve_all_records(session, request_url, reqHeaders)
    secondary_uom_df = pd.DataFrame(response)
    # secondary_uom_df = secondary_uom_df[~(secondary_uom_df['itemNo'].isna())]
    secondary_uom_df = secondary_uom_df.rename(columns={"itemNo": "System No.", "code": "UOM", "qtyPerUnitOfMeasure": "Conversion"})
    # secondary_uom_df['Conversion'] = secondary_uom_df['Conversion'].astype('float16')
    secondary_uom_df["UOM"] = secondary_uom_df["UOM"].astype("category")
    secondary_uom_df = secondary_uom_df.merge(
        secondary_uom_df[secondary_uom_df["Conversion"] == 1].drop_duplicates(subset=["System No."]).rename(columns={"UOM": "Base UOM"})[["System No.", "Base UOM"]], how="left", on=["System No."]
    )
    secondary_uom_df = secondary_uom_df[["System No.", "UOM", "Conversion", "Base UOM"]]
    secondary_uom_df.reset_index(drop=True)
    t_elapsed = time.time() - t_start
    print("Items UOM conversion table retrieved. It took %d seconds." % t_elapsed)

    return (secondary_uom_df, item_master_df)



def convert_bom_uom(bom_df, secondary_uom_df):
    bom_df["UOM"] = bom_df["UOM"].str.upper().str.strip()
    system_number_set = set(secondary_uom_df["System No."])

    def convert_uom_line(a):
        import numpy as np

        s = a.copy()
        if s.isna().all():  # skip empty lines other than first line
            return s
        if s["System No."] in system_number_set:
            s["Item Master Matched"] = True
        else:
            s["Item Master Matched"] = False
            s["UOM Valid"] = False
            # return s
        if s["UOM"] in set(secondary_uom_df.loc[secondary_uom_df["System No."] == s["System No."], "UOM"]):
            s["UOM Valid"] = True
            s["Conversion"] = secondary_uom_df.loc[np.logical_and(secondary_uom_df["System No."] == s["System No."], secondary_uom_df["UOM"] == s["UOM"]), "Conversion"].iloc[0]
            s["Qty"] = s["Qty"] * s["Conversion"]
            s["UOM"] = secondary_uom_df.loc[secondary_uom_df["System No."] == s["System No."], "Base UOM"].iloc[0]
        else:
            s["UOM Valid"] = False
        return s

    bom_df = bom_df.apply(convert_uom_line, axis=1)
    return bom_df


def flatten_bom(input_system_bom_df, input_contingency_bom_df, bom_statistics, verbose=False):
    system_bom_df = input_system_bom_df
    contingency_bom_df = input_contingency_bom_df

    if len(system_bom_df) > 0:
        system_bom_df = system_bom_df[system_bom_df["Keep"]].drop(columns=["Keep"]).reset_index(drop=True)

    if len(contingency_bom_df) > 0:
        contingency_bom_df = contingency_bom_df[contingency_bom_df["Keep"]].drop(columns=["Keep"]).reset_index(drop=True)

    # check for duplicate hier num
    if len(system_bom_df["Hierarchical No."].unique()) != len(system_bom_df["Hierarchical No."]):
        raise ValueError("Duplicate hierarchical number detected in System BOM for" + str(list(system_bom_df.loc[system_bom_df["Hierarchical No."].duplicated(), "Hierarchical No."])))
    elif len(contingency_bom_df["Hierarchical No."].unique()) != len(contingency_bom_df["Hierarchical No."]):
        raise ValueError("Duplicate hierarchical number detected in Contingency BOM" + str(list(contingency_bom_df.loc[contingency_bom_df["Hierarchical No."].duplicated(), "Hierarchical No."])))

    # recursively flatten bom qty
    system_bom_df["Flat Qty"] = 0.0
    contingency_bom_df["Flat Qty"] = 0.0
    for i in range(len(system_bom_df)):
        system_bom_df.at[i, "Flat Qty"] = flatten_qty(i, system_bom_df)
    system_bom_df["Qty"] = system_bom_df["Flat Qty"]  # overwrite qty with flattened qty
    system_bom_df = system_bom_df.drop(columns=["Flat Qty"])
    for i in range(len(contingency_bom_df)):
        contingency_bom_df.at[i, "Flat Qty"] = flatten_qty(i, contingency_bom_df)
    contingency_bom_df["Qty"] = contingency_bom_df["Flat Qty"]  # overwrite qty with flattened qty
    contingency_bom_df = contingency_bom_df.drop(columns=["Flat Qty"])

    # combine system and contingency bom and drop extra flat qty column
    non_obsolete_bom_df = pd.concat([system_bom_df, contingency_bom_df], axis=0)[["System No.", "Description", "Description 2", "Manufacturer", "Qty", "UOM", "Unit Cost [SGD]"]].reset_index(drop=True)

    # clean and aggregate BOM
    if (len(system_bom_df) == 0 and len(contingency_bom_df) == 0) or (np.all(system_bom_df["System No."].isna()) and np.all(contingency_bom_df["System No."].isna())):  # fully empty BOM
        flat_bom_df = pd.DataFrame([], columns=["System No.", "Description", "Description 2", "Manufacturer", "Qty", "UOM", "In FG", "In Contingency"])
    else:
        flat_bom_df = pd.DataFrame(pd.pivot_table(non_obsolete_bom_df[["System No.", "Qty"]], index=["System No."], aggfunc="sum").to_records())
        flat_bom_df = pd.merge(flat_bom_df, non_obsolete_bom_df[["System No.", "Description", "Description 2", "Manufacturer", "UOM", "Unit Cost [SGD]"]], on="System No.", how="left")
        flat_bom_df["In FG"] = False
        flat_bom_df["In Contingency"] = False
        # flat_bom_df = flat_bom_df[['System No.','Description','Manufacturer','Qty','UOM']]
        flat_bom_df = flat_bom_df[["System No.", "Description", "Description 2", "Manufacturer", "Qty", "UOM", "Unit Cost [SGD]", "In FG", "In Contingency"]].drop_duplicates(subset=["System No."])

        flat_bom_df["In FG"] = flat_bom_df["System No."].isin(system_bom_df["System No."])
        flat_bom_df["In Contingency"] = flat_bom_df["System No."].isin(contingency_bom_df["System No."])
        flat_bom_df = flat_bom_df.reset_index(drop=True)

    return (flat_bom_df, bom_statistics)  # convert UOM again in case user supplies sys and cont bom with incorrect UOMs




def generate_engg_punchlist(current_sys_bom_df, current_cont_bom_df, current_bom_stats=None, verbose=False):
    # initialize punchlist
    engineering_punchlist_df = pd.DataFrame(
        [],
        columns=[
            "PID",
            "PIC",
            "ME",
            "EE",
            "Planned CTB Date",
            "Planned Assembly Start Date",
            "System or Contingency BOM",
            "Hierarchical No.",
            "System No.",
            "Description",
            "Description 2",
            "Manufacturer",
            "Qty",
            "UOM",
            "Obsolete",
            "WIP or Released",
            "Item Master Matched",
            "UOM Valid",
            "SysNo-Description Match",
            "Keep",
        ],
    )

    # flatten bom
    (current_flat_bom_df, current_updated_bom_stats) = flatten_bom(current_sys_bom_df, current_cont_bom_df, current_bom_stats, verbose)
    current_flat_bom_df = current_flat_bom_df[np.logical_and(current_flat_bom_df["System No."].str.isalnum(), current_flat_bom_df["System No."].str.len() == 12)]

    try:

        # populate BOM lines punchlist for engineering
        current_sys_bom_problematic_lines = current_sys_bom_df[np.logical_and(current_sys_bom_df["Keep"] == False, current_sys_bom_df["Obsolete"] == "N")]
        current_cont_bom_problematic_lines = current_cont_bom_df[np.logical_and(current_cont_bom_df["Keep"] == False, current_cont_bom_df["Obsolete"] == "N")]

        current_sys_bom_problematic_lines["System or Contingency BOM"] = "System BOM"
        current_cont_bom_problematic_lines["System or Contingency BOM"] = "Contingency BOM"
        engineering_punchlist_df = pd.concat([engineering_punchlist_df, current_sys_bom_problematic_lines])
        engineering_punchlist_df = pd.concat([engineering_punchlist_df, current_cont_bom_problematic_lines])
        epcols = [
            "System or Contingency BOM",
            "Hierarchical No.",
            "System No.",
            "Description",
            "Description 2",
            "Manufacturer",
            "Qty",
            "UOM",
            "Obsolete",
            "WIP or Released",
            "Completeness",
            "Item Master Matched",
            "UOM Valid",
            "SysNo-Description Match",
            "KeepThisLine",
            "KeepParent",
            "Parent",
            "Keep",
        ]
        engineering_punchlist_df = engineering_punchlist_df[epcols]

    except:
        engineering_punchlist_df = pd.DataFrame()

    return engineering_punchlist_df


def bom_checker(bom_path: str):
    bom_url = bom_path.split("BOM%20Release/")[0]
    ctx = login_to_sharepoint(bom_url, username, password, verbose=False)
    output_ctx = login_to_sharepoint(output_sharepoint_site_url, username, password, verbose=False)

    try:
        sys_bom_df, cont_bom_df, bom_filename = get_raw_bom(bom_path, ctx, bom_url)
        sys_bom_sheet = None
        cont_bom_sheet = None
        sys_bom_df["Hierarchical No."] = sys_bom_df["Hierarchical No."].apply(handle_integer_hierarchical_numbers)
        cont_bom_df["Hierarchical No."] = cont_bom_df["Hierarchical No."].apply(handle_integer_hierarchical_numbers)
        (secondary_uom_df, item_master_df) = get_BC_data_api_generic(tenant_id, client_id, client_secret, environment)

        # grab expendables list from Sharepoint
        expendables_df = get_expendables_list(expendables_list_path, output_ctx, output_sharepoint_site_url, verbose=False)

        # Process System BOM
        sys_bom_df = process_hierarchical_bom(sys_bom_df, sys_bom_sheet, secondary_uom_df, item_master_df, expendables_df, verbose, False)
        cont_bom_df = process_hierarchical_bom(cont_bom_df, cont_bom_sheet, secondary_uom_df, item_master_df, expendables_df, verbose, False)
        return ("BOM Processed", sys_bom_df, cont_bom_df)
    except Exception as e:
        return ("BOM retrieval failed: " + e.__str__(), pd.DataFrame(), pd.DataFrame())


def flatten_qty(i, df):
    hier_num_str = df.at[i, "Hierarchical No."]
    df_top_level = df["Hierarchical No."].apply(lambda s: len(s.split("."))).min()
    if len(hier_num_str.split(".")) == df_top_level:  # top-level item
        return df.at[i, "Qty"]
    else:
        parent_hier_num_str = ".".join(hier_num_str.split(".")[:-1])
        if not parent_hier_num_str in set(df["Hierarchical No."]):
            raise ValueError("Hierarchical number error (parent not found) detected for item: " + hier_num_str)
        parent_df_index = np.where(np.logical_and(np.logical_and(df["Hierarchical No."] == parent_hier_num_str, df["Obsolete"] == "N"), df["WIP or Released"] == "Released"))[0]
        if len(parent_df_index) > 1:
            raise ValueError("Duplicate parent detected for item: " + hier_num_str)
        elif len(parent_df_index) == 0:
            raise ValueError("Parent WIP or obsoleted, but this item is not WIP nor obsoleted ")
        else:
            parent_df_index = parent_df_index[0]
        parent_qty = flatten_qty(parent_df_index, df)
        return parent_qty * df.at[i, "Qty"]


def keep_or_drop(i, df):
    """Returns a boolean that states whether to keep this row"""
    hier_num_str = df.at[i, "Hierarchical No."]
    if type(hier_num_str) != str or hier_num_str == None:
        return (False, False, False)  # drop lines without hierarchical number

    df_top_level = df.loc[~(df["Hierarchical No."].isna()), "Hierarchical No."].apply(lambda s: len(s.split("."))).min()

    # dropping logic
    keep_this_row = not (
        df.at[i, "Obsolete"] == "Y"
        or df.at[i, "WIP or Released"] != "Released"
        or ((df.at[i, "UOM Valid"] == False) and (df.at[i, "Item Master Matched"] == True))
        or df.at[i, "Completeness"] == False
        or not (df.at[i, "SysNo-Description Match"] in ["Validated", "New item", "Non-inventorized Expendable"])
    )

    if len(hier_num_str.split(".")) == df_top_level:  # top-level item, just decide whether this line is valid
        return (keep_this_row, keep_this_row, True)
    else:  # not top-level, need to recursively walk up the BOM
        parent_hier_num_str = ".".join(hier_num_str.split(".")[:-1])
        if not parent_hier_num_str in set(df["Hierarchical No."]):
            raise ValueError("Hierarchical number error (parent not found) detected for item: " + hier_num_str)
        parent_df_index = np.where(df["Hierarchical No."] == parent_hier_num_str)[0]
        if len(parent_df_index) > 1:  # deal with multiple parent hierarchical numbers found
            if len(np.where(np.logical_and(np.logical_and(df["Hierarchical No."] == parent_hier_num_str, df["Obsolete"] == "N"), df["WIP or Released"] == "Released"))[0]) > 1:
                raise ValueError("Duplicate parent detected for item: " + hier_num_str)
            elif len(np.where(np.logical_and(np.logical_and(df["Hierarchical No."] == parent_hier_num_str, df["Obsolete"] == "N"), df["WIP or Released"] == "Released"))[0]) < 1:
                parent_df_index = parent_df_index[0]
            else:
                parent_df_index = np.where(np.logical_and(np.logical_and(df["Hierarchical No."] == parent_hier_num_str, df["Obsolete"] == "N"), df["WIP or Released"] == "Released"))[0][0]
        else:
            parent_df_index = parent_df_index[0]
        keep_parent = keep_or_drop(parent_df_index, df)[0]

        return (keep_this_row and keep_parent, keep_this_row, keep_parent)


def process_hierarchical_bom(bom_df, bom_sheet, secondary_uom_df, item_master_df, expendables_df, verbose=False, backfill=False):
    """
    cleans and processes a raw bom by:
        - rename columns to canonical form
        - completeness check on full BOM
        - UOM conversion on full BOM
        - computing parent line by line
        - matching sys num to desc line by line
        - matching unit cost to sys num line by lne
        - recursively calculate hardware costs of assemblies
        - arranging columns into canonical order
    """
    # Preliminary checks
    if backfill and bom_sheet == None:
        raise ValueError("If backfilling is required, XLWings Sheet object must be passed")

    # Define allowed item categories
    valid_item_categories = {
        "3DP3DP",
        "AACACB",
        "AACACW",
        "ABPAAL",
        "ABPAAP",
        "ABPAPA",
        "ABPSLM",
        "ABPWIP",
        "ADAAIB",
        "ADAPIB",
        "AROMED",
        "BSMDGB",
        "BSMXRB",
        "BTSBTS",
        "BTSPAC",
        "CMM3DM",
        "CMMAMT",
        "CMMCMT",
        "CMMDEN",
        "CMMDMF",
        "CMMELA",
        "CMMGLF",
        "CMMKNS",
        "CMMPSE",
        "CMMSAC",
        "CMMTFC",
        "CNCTNH",
        "CNDACD",
        "CNDACS",
        "CNDAGT",
        "CNDCPY",
        "CNDELM",
        "CNDFAG",
        "CNDHES",
        "CNDMIT",
        "CNDPAN",
        "CNDPCA",
        "CNDSTD",
        "CNDTRU",
        "DROPSH",
        "EEPCNW",
        "EEPDPI",
        "EEPECA",
        "EEPEIP",
        "EEPICT",
        "EEPLSS",
        "EEPPWR",
        "EEPRMA",
        "ENCABA",
        "ENCABA",
        "ENCABI",
        "ENCABI",
        "ENCABS",
        "ENCAKS",
        "ENCAKS",
        "ENCARA",
        "ENCBOG",
        "ENCEBI",
        "ENCEIM",
        "ENCERA",
        "ENCETB",
        "ENCGSS",
        "ENCHES",
        "ENCLAE",
        "ENCNRA",
        "ENCRES",
        "ENCSEK",
        "FABFIX",
        "JIGJIG",
        "LMCACM",
        "LMCACM",
        "LMCACR",
        "LMCAHM",
        "LMCAHM",
        "LMCAJM",
        "LMCAKD",
        "LMCAKH",
        "LMCAKM",
        "LMCAKM",
        "LMCAKS",
        "LMCALM",
        "LMCAMF",
        "LMCAML",
        "LMCAMS",
        "LMCAPM",
        "LMCAPR",
        "LMCAPZ",
        "LMCAQM",
        "LMCATM",
        "LMCAUL",
        "LMCAUM",
        "LMCAWM",
        "LMCCLA",
        "LMCCLC",
        "LMCRDM",
        "LMMAUM",
        "LMMDBG",
        "LMMDGC",
        "LMMDGE",
        "LMMDGF",
        "LMMDGH",
        "LMMDGL",
        "LMMSGL",
        "LMMVPL",
        "LMMWEG",
        "LMMWGL",
        "LMMXRL",
        "LMMXRV",
        "LMPAUM",
        "LMTACM",
        "LMTACR",
        "LMTAJM",
        "LMTAKD",
        "LMTAKH",
        "LMTAKM",
        "LMTAKS",
        "LMTALM",
        "LMTAMS",
        "LMTAPM",
        "LMTAPZ",
        "LMTAQM",
        "LMTATM",
        "LMTAUL",
        "LMTAUM",
        "LMTAWM",
        "LMTCLA",
        "LMTCLC",
        "LMTNPM",
        "LMTPTC",
        "LMTRDM",
        "MASGST",
        "MASPGS",
        "MASTGS",
        "MASVRG",
        "MEPDVE",
        "MEPERC",
        "MEPFSC",
        "MEPLMB",
        "MEPPNA",
        "MEPRBR",
        "MEPVIO",
        "MRMABA",
        "MRMABM",
        "MRMADU",
        "MRMAEX",
        "MRMFBA",
        "MRMFBS",
        "MRMFSS",
        "MRMHBA",
        "MRMHBB",
        "MRMHRM",
        "MRMHSA",
        "MRMHSB",
        "MRMLMG",
        "MRMPLA",
        "MRMPLM",
        "MRMPLS",
        "MRMRBA",
        "MRMRBD",
        "MRMRBM",
        "MRMRBS",
        "MRMSBA",
        "MRMSIS",
        "MRMUTP",
        "MSTAMG",
        "MSTAML",
        "MSTAMR",
        "MSTAMS",
        "MSTAMZ",
        "MSTASY",
        "MSTVAC",
        "MSTXRG",
        "OTHOTH",
        "PRMCUW",
        "PRMEPX",
        "PRMLAM",
        "PRMMAG",
        "PRMMIS",
        "PRMPGM",
        "PRMTDM",
        "RBTARJ",
        "RBTARS",
        "RBTRPZ",
        "RMARMA",
        "RMMACD",
        "RMMACW",
        "RMMADR",
        "RMMAGR",
        "RMMARC",
        "RMMASA",
        "RMMATR",
        "RMMAXD",
        "RMMAXM",
        "RMMCRM",
        "ROCACD",
        "ROCACW",
        "ROCADR",
        "ROCAER",
        "ROCAPR",
        "ROCATR",
        "ROCCRC",
        "ROMACD",
        "ROMACW",
        "ROMADR",
        "ROMAER",
        "ROMAMS",
        "ROMAPR",
        "ROMATR",
        "ROMAXD",
        "ROMCRC",
        "RSMACD",
        "RSMACW",
        "RSMADR",
        "RSMAER",
        "RSMATR",
        "RSMCRC",
        "SAMHSM",
        "SAMJIG",
        "SAMMBC",
        "SAMMCF",
        "SAMMFC",
        "SAMMHC",
        "SAMMMC",
        "SAMMTR",
        "SAMMWH",
        "SAMMWS",
        "SAMRCL",
        "SAMRMG",
        "SAMRRY",
        "SAMVCA",
        "SAMVMA",
        "SASBAS",
        "SASCAR",
        "SASSSM",
        "SEN3PP",
        "SENACC",
        "SENGEO",
        "SENLIM",
        "SMSAPC",
        "SMSAPK",
        "SMSATP",
        "SSPSSP",
        "SSPSSP",
        "SSPSSP",
        "STDAPK",
        "STDAZT",
        "SVCSEI",
        "VBIHES",
        "VBIIDE",
        "VCM3PP",
        "VCMCVM",
        "VCMDGV",
        "VCMLFK",
        "VCMMBV",
        "VCMMGV",
        "VCMTGV",
        "VCMTHF",
        "VCMXCV",
        "VCMXMG",
        "VCMXMG",
        "VCMXRS",
        "VCMXRV",
        "VCMXRZ",
        "VOCAML",
        "VOCATA",
        "VOCAVA",
        "VOCAVM",
        "VOCAVR",
        "VOCBMC",
        "VOCCCA",
        "VOCCMA",
        "VOCCVP",
        "VOCCVR",
        "VOCDPT",
        "VOCFRZ",
        "VOCLFC",
        "VOCLRH",
        "VOCMSX",
        "VOCOCA",
        "VOCRXS",
        "VOCTFC",
        "VOCTHF",
        "VOCZLC",
        "VOCZRC",
        "VOMAML",
        "VOMATA",
        "VOMAVA",
        "VOMAVM",
        "VOMAVR",
        "VOMCVP",
        "VOMCVR",
        "VOMFRZ",
        "VOMLFA",
        "VOMMFA",
        "VOMMSX",
        "VOMTFC",
        "VOMTHF",
        "VOMVCM",
        "VOMZLC",
        "VOMZRC",
        "VSM2RZ",
        "VSMATA",
        "VSMAVA",
        "VSMAVM",
        "VSMAVR",
        "VSMCVC",
        "VSMCVP",
        "VSMCVR",
        "VSMLFK",
        "VSMLRH",
        "VSMMSX",
        "VSMTHF",
        "FABSRL",
        "PKGPKG",
        "CLNCLN",
    }

    # Rename columns
    bom_df["Qty"] = bom_df["Qty"].astype("float")
    bom_df["Unit Cost [SGD]"] = bom_df["Unit Cost [SGD]"].astype("float")
    bom_df["Total Cost [SGD]"] = bom_df["Total Cost [SGD]"].astype("float")
    bom_df = bom_df.rename(columns={"Description\n(Order Part No / Dwg No / REV No.)": "Description", "Description 2\n(Description / Dwg Title)": "Description 2"})
    bom_df["Description"] = bom_df["Description"].str.strip()
    bom_df["Description"] = bom_df["Description"].str.upper()
    bom_df["System No."] = bom_df["System No."].astype("str")
    bom_df["System No."] = bom_df["System No."].str.strip()

    # Delete all system numbers claiming to be non-inventorized expendables (expendables list is SSOT)
    bom_df.loc[bom_df["System No."].str.startswith("SVC"), "System No."] = ""

    bom_df["SysNo-Description Match"] = ""
    if "Obselete" in bom_df.keys():
        bom_df = bom_df.rename(columns={"Obselete": "Obsolete"})

    if len(bom_df) > 0:
        # Completeness check
        bom_df["Completeness"] = ~(bom_df["Description"].isna() | bom_df["Description 2"].isna() | bom_df["Qty"].isna() | bom_df["UOM"].isna() | bom_df["Manufacturer"].isna())

        # iterate through index, note not to drop index after dropna so that the index still matches Excel row numbers
        for i in bom_df.index:  # First iteration through... Match system numbers
            if np.all(bom_df.loc[i].isna()):  # skip empty rows
                continue

            # Compute parent
            if bom_df.loc[i, "Hierarchical No."] == None:
                bom_df.loc[i, "Parent"] = ""
            else:
                bom_df.loc[i, "Parent"] = ".".join(bom_df.loc[i, "Hierarchical No."].split(".")[:-1])

            # Match Description to SysNum
            sys_no_match_result = match_sys_no_description(bom_df.loc[i, "Description"], item_master_df)
            if not (type(bom_df.loc[i, "System No."]) == str and len(bom_df.loc[i, "System No."]) == 12):  # complete system number not provided in BOM
                if sys_no_match_result[0] == "No Match":
                    if (
                        type(bom_df.loc[i, "System No."]) == str
                        and (len(bom_df.loc[i, "System No."]) == 6 and bom_df.loc[i, "System No."] in valid_item_categories)
                        or (len(bom_df.loc[i, "System No."]) == 3 and bom_df.loc[i, "System No."].startswith("FAB"))
                    ):
                        bom_df.loc[i, "SysNo-Description Match"] = "New item"
                    else:
                        bom_df.loc[i, "SysNo-Description Match"] = "New item, pending designer to assign item category"
                elif sys_no_match_result[0] == "Unique Match":
                    bom_df.loc[i, "System No."] = sys_no_match_result[1][0]
                    bom_df.loc[i, "SysNo-Description Match"] = "Validated"
                elif sys_no_match_result[0] == "Duplicated Match":
                    bom_df.loc[i, "SysNo-Description Match"] = "Description duplicated in item master, manual disambiguation needed"
            else:  # complete system number provided in BOM
                if sys_no_match_result[0] == "No Match":
                    bom_df.loc[i, "SysNo-Description Match"] = "Invalid SysNo-Description match"
                elif sys_no_match_result[0] == "Unique Match":
                    if bom_df.loc[i, "System No."] == sys_no_match_result[1][0]:
                        bom_df.loc[i, "SysNo-Description Match"] = "Validated"
                    else:
                        bom_df.loc[i, "SysNo-Description Match"] = "Invalid SysNo-Description match"
                elif sys_no_match_result[0] == "Duplicated Match":
                    if bom_df.loc[i, "System No."] in sys_no_match_result[1]:
                        bom_df.loc[i, "SysNo-Description Match"] = "Validated"
                    else:
                        bom_df.loc[i, "SysNo-Description Match"] = "Invalid SysNo-Description match"  # system number does not match BC

            # Match Unit Cost to SysNum
            if bom_df.loc[i, "SysNo-Description Match"] != "Validated":  # non-validated system number, unit cost is unknown
                bom_df.loc[i, "Unit Cost [SGD]"] = 0
            else:
                unit_cost_match_result = match_unit_cost(bom_df.loc[i, "System No."], item_master_df)
                if unit_cost_match_result[0] != "Unique Match":  # duplicate in item master, error condition
                    bom_df.loc[i, "Unit Cost [SGD]"] = 0
                    raise ValueError("Duplicate system number detected in item_master_df")
                # elif (bom_df.loc[i,'Unit Cost [SGD]'] in {None, np.nan, 0, 0.0}) or math.isnan(bom_df.loc[i,'Unit Cost [SGD]']): #unit cost field is empty
                else:
                    bom_df.loc[i, "Unit Cost [SGD]"] = unit_cost_match_result[1]  # synchronize imported dataframe with sheet

            # identify expendables based on description and assign SVC number
            if bom_df.loc[i, "Description"] in set(expendables_df["Description"]):
                bom_df.loc[i, "System No."] = "SVCSEI0A0001"
                bom_df.loc[i, "SysNo-Description Match"] = "Non-inventorized Expendable"

        # UOM Conversion
        bom_df = convert_bom_uom(bom_df, secondary_uom_df)

        for i in bom_df.index:  # Second iteration through... Compute keep or drop
            # exempt SVC items from UOM validity check
            if bom_df.loc[i, "SysNo-Description Match"] in {"Non-inventorized Expendable"}:
                bom_df.loc[i, "UOM Valid"] = True
                bom_df.loc[i, "Item Master Matched"] = False
                bom_df.loc[i, "Unit Cost [SGD]"] = 0

            # Compute keep or drop
            Keep, KeepThisLine, KeepParent = keep_or_drop(i, bom_df)
            bom_df.loc[i, "KeepThisLine"] = KeepThisLine
            bom_df.loc[i, "KeepParent"] = KeepParent
            bom_df.loc[i, "Keep"] = Keep

        # bom_df = bom_df.apply(process_one_line,axis=1,args=(item_master_df)) #TO-DO: try to wrap the process on a single BOM line in a function to allow parallelization

        # recursively update assembly hardware costs
        for i in bom_df[bom_df["Parent"] == ""].index:  # iterate through top level assemblies and recursively update their costs
            if bom_df.loc[i, "Hierarchical No."] == None:
                continue
            try:
                recursive_update_assy_cost(i, bom_df)
            except Exception as e:
                raise e
                bom_df.loc[i, "Unit Cost [SGD]"] = 0

        # compute total cost
        bom_df["Total Cost [SGD]"] = bom_df["Unit Cost [SGD]"] * bom_df["Qty"]

        # backfill sys num and cost at one go
        if backfill:
            bom_sheet["B13"].options(transpose=True).value = bom_df["System No."].to_numpy()
            bom_sheet["C13"].options(transpose=True).value = bom_df["Description"].to_numpy()
            bom_sheet["G13"].options(transpose=True).value = bom_df["Unit Cost [SGD]"].to_numpy()
            bom_sheet["H13"].options(transpose=True).value = bom_df["Total Cost [SGD]"].to_numpy()

        # drop empty lines and sort hier num
        bom_df = bom_df.dropna(axis=0, how="all")
        bom_df = bom_df.dropna(axis=0, subset=["Hierarchical No."])  # drop lines with blank hiererchical number
        bom_df = bom_df.sort_values("Hierarchical No.", key=lambda x: np.argsort(index_natsorted(bom_df["Hierarchical No."]))).reset_index(drop=True)  # BOM is non-empty, process as usual

    else:  # just create necessary empty columns for empyt BOM
        bom_df["Completeness"] = ""
        bom_df["Keep"] = ""
        bom_df["KeepThisLine"] = ""
        bom_df["KeepParent"] = ""
        bom_df["Parent"] = ""
        bom_df["Item Master Matched"] = ""
        bom_df["UOM Valid"] = ""

    # arrange columns into canonical order
    bom_df = bom_df[
        [
            "Hierarchical No.",
            "System No.",
            "Description",
            "Description 2",
            "Manufacturer",
            "Qty",
            "UOM",
            "Unit Cost [SGD]",
            "Parent",
            "Obsolete",
            "WIP or Released",
            "Item Master Matched",
            "UOM Valid",
            "Completeness",
            "SysNo-Description Match",
            "KeepThisLine",
            "KeepParent",
            "Keep",
        ]
    ]

    return bom_df


def match_sys_no_description(description, item_master_df):
    """Finds system number of a given BOM line given its description (aka MPN)"""
    description_matches_df = item_master_df[item_master_df["Description"] == description].reset_index(drop=True)

    if len(description_matches_df) == 1:  # uniquely matched
        return ("Unique Match", [description_matches_df.loc[0, "System No."]])
    elif len(description_matches_df) == 0:  # no match
        return ("No Match", None)
    else:  # non-unique match
        return ("Duplicated Match", list(description_matches_df["System No."]))


def match_unit_cost(sys_num, item_master_df):
    """Finds last-known unit cost of a given item from item master based on its system number"""
    sys_num_matches_df = item_master_df[item_master_df["System No."] == sys_num].reset_index(drop=True)

    if len(sys_num_matches_df) == 1:  # uniquely matched
        return ("Unique Match", sys_num_matches_df.loc[0, "Unit Cost"])
    elif len(sys_num_matches_df) == 0:  # no match
        return ("No Match", None)
    else:  # non-unique match
        return ("Duplicated Match", None)




def recursive_update_assy_cost(i, df):
    """idempotently overwrites the dataframe unit cost fields!!! recursively calculates the hardware cost of an assembly (qty = 1) as the sum of the cost of its constituent child parts"""
    if not (is_assy(i, df)):  # is a child part
        # no overwriting needed since this this part is a child
        return df.loc[i, "Unit Cost [SGD]"]  # is a child part
    else:  # is an assembly (i.e. has child parts)
        sub_bom_df = df[np.logical_and(df["Parent"] == df.loc[i, "Hierarchical No."], df["Keep"] == True)]
        for child_hier_num in sub_bom_df["Hierarchical No."]:
            if child_hier_num in set(df["Parent"]):
                child_hier_num_index_in_full_bom_df = df[np.logical_and(df["Hierarchical No."] == child_hier_num, df["Keep"] == True)].index.tolist()
                if len(child_hier_num_index_in_full_bom_df) > 1:
                    raise ValueError("Duplicate hier num detected in full bom df")
                elif len(child_hier_num_index_in_full_bom_df) == 0:
                    raise ValueError("Child hier num not found in full bom df")
                recursive_update_assy_cost(child_hier_num_index_in_full_bom_df[0], df)  # update the cost for the child which is an assy
        # now that all child assy costs have been updated, update the parent assy cost
        df.loc[i, "Unit Cost [SGD]"] = (
            df.loc[np.logical_and(df["Parent"] == df.loc[i, "Hierarchical No."], df["KeepThisLine"] == True), "Unit Cost [SGD]"]
            * df.loc[np.logical_and(df["Parent"] == df.loc[i, "Hierarchical No."], df["KeepThisLine"] == True), "Qty"]
        ).sum()
        return df.loc[i, "Unit Cost [SGD]"]


def is_assy(i, df):
    return df.loc[i, "Hierarchical No."] in set(df["Parent"])
