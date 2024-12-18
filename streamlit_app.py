from utils.duckdb import duckdb_setup
from utils.st import st_class
import streamlit as st

if __name__ == "__main__":
    duckdb_setup.run_db(year="2017", order_status="delivered")
    st_class.run_app()
