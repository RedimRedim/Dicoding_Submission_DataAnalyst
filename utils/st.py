import streamlit as st
from utils.duckdb import duckdb_setup
from pandas import DataFrame
import matplotlib.pyplot as plt


class StreamLitClass:
    def __init__(self):
        self.year_selection = None
        self.order_status_selection = None

    def run_app(self) -> None:
        st.title("Data Analyst Testing")
        self._add_sidebar()
        self._st_pie_chart()
        self._st_bar_chart()

    def _add_sidebar(self) -> DataFrame:
        with st.sidebar:
            st.text("Example Sidebar")

            self.year_selection = st.selectbox(
                "Select Year",
                options=sorted(duckdb_setup.product_df["year"].unique()),
            )

            self.order_status_selection = st.selectbox(
                "Select Order Status",
                options=sorted(duckdb_setup.product_df["order_status"].unique()),
            )

            # re-run dataframe when selection change
            duckdb_setup.run_db(self.year_selection, self.order_status_selection)

    def _st_pie_chart(self) -> None:
        st.subheader(
            f"{self.year_selection} Order_Ratio based in Payment_Type - {self.order_status_selection} "
        )
        fig, ax = plt.subplots()
        ax.pie(
            x=duckdb_setup.payment_type_result_df["order_ratio"],
            labels=duckdb_setup.payment_type_result_df["payment_type"],
            autopct="%1.1f%%",
            startangle=90,
        )

        ax.axis("equal")
        st.pyplot(fig)

    def _st_bar_chart(self) -> None:
        try:
            payment_most_installments_header = (
                duckdb_setup.payment_result_most_installments["payment_installments"][0]
            )
        except:
            payment_most_installments_header = ""

        st.subheader(
            f"{ self.year_selection} Credit Card Top 5 Products - Total Orders Most Installments: {payment_most_installments_header} "
        )

        fig, ax = plt.subplots()

        x = duckdb_setup.payment_result_most_installments["product_category_name"]
        y = duckdb_setup.payment_result_most_installments["total_order"]
        ax.bar(x, y)

        for i, value in enumerate(y):
            ax.text(
                i, value + 2, str(value), ha="center", va="bottom", fontsize=10
            )  # Adjust spacing as needed

        # Set axis labels and title
        ax.set_xlabel("Product Category", fontsize=12)
        ax.set_ylabel("Total Orders", fontsize=12)

        # Rotate x-tick labels for readability
        plt.xticks(rotation=45, ha="right")

        # Adjust layout to fit labels
        plt.tight_layout()

        # Render the plot in Streamlit
        st.pyplot(fig)


st_class = StreamLitClass()
