import streamlit as st
from utils.duckdb import duckdb_setup
from pandas import DataFrame


class StreamLitClass:
    def __init__(self):
        pass

    def run_app(self) -> None:
        st.title("Data Analyst Testing")
        filtered_df = self._add_sidebar()
        self._add_dataframe(duckdb_setup.payment_type_result_df)
        self._add_dataframe(duckdb_setup.payment_installments_all)
        self._add_dataframe(filtered_df)

    def _add_sidebar(self) -> DataFrame:

        with st.sidebar:
            st.text("Example Sidebar")
            return self._sidebar_month_installments()

    def _sidebar_month_installments(self) -> DataFrame:
        selected_installment = st.selectbox(
            label="Payment Installments",
            options=sorted(
                duckdb_setup.payment_result_installments_df[
                    "payment_installments"
                ].unique()
            ),
        )
        filtered_df = duckdb_setup.filtered_payment_result_installments_df(
            selected_installment
        )

        return filtered_df

    def _add_dataframe(self, df) -> None:
        st.dataframe(df)


st_class = StreamLitClass()
