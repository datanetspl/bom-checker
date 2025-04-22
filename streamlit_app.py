"""
PLEASE INCLUDE SECRETS.PKL IN THIS FOLDER
"""

import streamlit as st
import sys
import pandas as pd
from utils import generate_engg_punchlist, bom_checker, client_id, tenant_id
from msal_streamlit_authentication import msal_authentication


engg_punchlist = pd.DataFrame()

# Wide page to view entire Punchlist
st.set_page_config(layout="wide")

# Add State for Punchlist DataFrame
if "punchlist" not in st.session_state:
    st.session_state["punchlist"] = 0

st.title("BOM CHECKER")

login_token = msal_authentication(
    auth={
        "clientId": client_id,
        "authority": f"https://login.microsoftonline.com/{tenant_id}",
        "redirectUri": "https://bomchecker.streamlit.app",
        "postLogoutRedirectUri": "https://bomchecker.streamlit.app",
    },
    cache={"cacheLocation": "sessionStorage", "storeAuthStateInCookie": False},
    login_button_text="Login",
    logout_button_text="Logout",
    class_name="css_button_class_selector",
    html_id="html_id_for_button",
)
#st.write("Login token:", login_token)

if not login_token:
    st.error("You must be logged in to use this app.")
    st.stop()

st.write(f"Welcome {login_token['account']['name']}!")

# Form to enter full BOM Path
with st.form("bom_url_form"):
    st.write("BOM Checker")
    bom_path = st.text_input(
        "Enter BOM Sharepoint URL",
        "https://akribissg.sharepoint.com/sites/StageteamSG544/BOM%20Release/2024/2403042%20SEMICAPS%20Verigy%20Direct%20Dock%20Modification/2403042%20SEMICAPS%20Verigy%20Direct%20Dock%20Modification.xlsx",
    )
    submitted = st.form_submit_button("Submit")
    if submitted:
        with st.spinner("Processing...", show_time=True):
            processed, sys_bom_df, cont_bom_df = bom_checker(bom_path)
            st.write(processed)
        if (processed == "BOM Processed") and (len(sys_bom_df) > 0):
            with st.spinner("Generating Punchlist...", show_time=True):
                engg_punchlist = generate_engg_punchlist(sys_bom_df, cont_bom_df)
                st.session_state["punchlist"] = 1

if st.session_state["punchlist"] == 1:
    if len(engg_punchlist) > 0:
        st.header("Punchlist")
        st.dataframe(engg_punchlist)
    else:
        st.write("No Punchlist Items")



