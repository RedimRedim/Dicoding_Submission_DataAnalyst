import streamlit as st
from pandas import DataFrame


class DuckDbSetup:
    def __init__(self):
        self.conn = None
        self.product_df = None
        self.payment_type_result_df = None
        self.payment_result_most_installments = None
        self.payment_result_most_installments_yearly = None

    def run_db(self, year, order_status) -> None:
        self._init_db()
        self.product_df = self._get_product_df()
        self.payment_type_result_df = self._get_payment_type_result_df(
            year, order_status
        )
        self.payment_result_most_installments = self._payment_result_most_installments(
            year, order_status
        )

    def _init_db(self) -> None:
        import duckdb

        try:
            if not self.conn:
                self.conn = duckdb.connect()
                self.conn.sql("INSTALL spatial")
                self.conn.sql("LOAD spatial")
        except Exception as e:
            print(f"An error occrued during DuckDB setup: {e}")
            return False

    def _get_product_df(self) -> DataFrame:
        product_df = self.conn.execute(
            """SELECT item_dataset.*,  product_category_name, order_approved_at,     
        EXTRACT(YEAR FROM CAST(order_approved_at AS DATE)) AS year, 
        order_status FROM './datasets/olist_order_items_dataset.csv' as item_dataset
        LEFT JOIN './datasets/olist_products_dataset.csv' as product_dataset
        ON product_dataset.product_id = item_dataset.product_id 
        LEFT JOIN './datasets/olist_orders_dataset.csv' as order_dataset
        ON order_dataset.order_id = item_dataset.order_id
        WHERE year is not null
           """
        ).fetchdf()

        product_df["year"] = product_df["year"].astype(
            "Int64"
        )  # Converts to nullable integer type

        product_df = product_df[
            product_df["order_status"].isin(["delivered", "shipped", "canceled"])
        ]

        product_df = product_df[product_df["year"].isin([2017, 2018])]

        # Register the DataFrame as a table in DuckDB for further use
        self.conn.register("product_df", product_df)

        return product_df

    def _get_payment_type_result_df(self, year, order_status) -> DataFrame:
        payment_type_result_df = self.conn.execute(
            f"""
                         SELECT payment_type, 
                         count(product_df.order_id) as total_order, 
                         sum(cast (payment_value as decimal)) as total_payment,
                         round((count(product_df.order_id) / sum(count(product_df.order_id)) over ()) * 100,0)  as order_ratio,
                         round((sum(payment_value) / sum(sum(payment_value)) over ()) * 100,0) as payment_value_ratio
                         FROM './datasets/olist_order_payments_dataset.csv' as payment_dataset
                         INNER JOIN product_df 
                         ON product_df.order_id = payment_dataset.order_id and extract(year from order_approved_at) = '{year}'
                         WHERE order_status like '%{order_status}%' 
                         GROUP BY payment_type
                         ORDER BY count(product_df.order_id) DESC
                      """
        ).fetchdf()

        self.conn.register("payment_type_result_df", payment_type_result_df)

        return payment_type_result_df

    def _payment_result_most_installments(self, year, order_status) -> DataFrame:
        query = f"""
            -- finding most using payment_installments
             With most_payment_installments as ( 
             SELECT payment_installments, sum(payment_value) as total_payment
             FROM './datasets/olist_order_payments_dataset.csv'
             WHERE  payment_type  like '%credit_card%' 
             group by payment_installments
             order by total_payment desc limit 1),
             
             -- getting product based in  most_payment_installments to find product_category_name
             order_output as (
               SELECT * FROM './datasets/olist_order_payments_dataset.csv' as payment_installments_dataset
               WHERE  payment_type  like '%credit_card%' and payment_installments = (select payment_installments from most_payment_installments) 
             ),
             
             -- getting product name based on order_id
             product_category_output as (
               select year, payment_installments, product_df.product_category_name, count(order_output.order_id) as total_order, sum(payment_value) as total_payment from order_output
               INNER JOIN product_df ON order_output.order_id = product_df.order_id
              WHERE order_status like '%{order_status}%'  and extract(year from order_approved_at) = '{year}'
               group by year, payment_installments , product_df.product_category_name, year
               order by total_payment desc limit 5
             )
             
             select * from product_category_output
             order by total_order desc
            """

        payment_result_most_installments = self.conn.execute(query).fetch_df()

        return payment_result_most_installments


# Export the instance
duckdb_setup = DuckDbSetup()
