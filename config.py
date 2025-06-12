import streamlit as st
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

 
def get_connection():
    private_key_path = st.secrets["credentials"]["private_key_path"]

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )

    pk_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake.connector.connect(
        account=st.secrets["credentials"]["account"],
        user=st.secrets["credentials"]["user"],
        private_key=pk_bytes,
        warehouse=st.secrets["credentials"]["warehouse"],
        database=st.secrets["credentials"]["database"],
        schema=st.secrets["credentials"]["schema"],
        role=st.secrets["credentials"]["role"]
    )
    return conn
