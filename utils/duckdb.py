import streamlit as st
from pandas import DataFrame


class DuckDbSetup:
    def __init__(self):
        self.conn = None
        self.product_df = None
        self.payment_type_result_df = None
        self.payment_result_installments_df = None
        self.payment_result_most_installments_yearly_df = None

    def run_db(self) -> None:
        self._init_db()
        self.product_df = self._get_product_df()
        self.payment_type_result_df = self._get_payment_type_result_df()
        self.payment_result_installments_df = self._payment_result_installments()
        self.payment_installments_all = self._payment_installments_all()

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
            """SELECT item_dataset.*,  product_category_name, order_approved_at, order_status FROM './datasets/olist_order_items_dataset.csv' as item_dataset
                          LEFT JOIN './datasets/olist_products_dataset.csv' as product_dataset
                          ON product_dataset.product_id = item_dataset.product_id
                          LEFT JOIN './datasets/olist_orders_dataset.csv' as order_dataset
                          ON order_dataset.order_id = item_dataset.order_id
           """
        ).fetchdf()

        # Register the DataFrame as a table in DuckDB for further use
        self.conn.register("product_df", product_df)

        return product_df

    def _get_payment_type_result_df(self) -> DataFrame:
        payment_type_result_df = self.conn.execute(
            """
                         SELECT payment_type, 
                         count(product_df.order_id) as total_order, 
                         sum(cast (payment_value as decimal)) as total_payment,
                         round((count(product_df.order_id) / sum(count(product_df.order_id)) over ()) * 100,0)  as order_ratio,
                         round((sum(payment_value) / sum(sum(payment_value)) over ()) * 100,0) as payment_value_ratio
                         FROM './datasets/olist_order_payments_dataset.csv' as payment_dataset
                         INNER JOIN product_df 
                         ON product_df.order_id = payment_dataset.order_id
                         WHERE order_status like '%delivered%'
                         GROUP BY payment_type
                         ORDER BY count(product_df.order_id) DESC
                      """
        ).fetchdf()

        self.conn.register("payment_type_result_df", payment_type_result_df)

        return payment_type_result_df

    def _payment_result_installments(self) -> DataFrame:
        payment_result_installments_df = self.conn.execute(
            """
            -- getting product based in  most_payment_installments to find product_category_name
            With order_output as (
              SELECT * FROM './datasets/olist_order_payments_dataset.csv' as payment_installments_dataset
              WHERE  payment_type  like '%credit_card%' 
            ),
            
            -- getting product name based on order_id
            product_category_output as (
              select payment_installments, product_df.product_category_name, count(order_output.order_id) as total_order, sum(payment_value) as total_payment from order_output
              INNER JOIN product_df ON order_output.order_id = product_df.order_id
            WHERE order_status like '%delivered%'
              group by payment_installments , product_df.product_category_name
              order by payment_installments, product_df.product_category_name
            )
            
            select * from product_category_output
            order by total_order desc
            """
        ).fetchdf()

        self.conn.register(
            "payment_result_installments_df", payment_result_installments_df
        )

        return payment_result_installments_df

    def _payment_installments_all(self):
        payment_installments_all_df = self.conn.execute(
            """
        SELECT payment_installments, count(total_order) as total_order, 
        sum(total_payment) as total_payment
        FROM payment_result_installments_df
        GROUP by payment_installments
        ORDER BY payment_installments asc
        """
        ).fetch_df()

        self.conn.register("payment_installments_all_df", payment_installments_all_df)

        return payment_installments_all_df

    def filtered_payment_result_installments_df(
        self, selected_installment
    ) -> DataFrame:

        filtered_df = self.payment_result_installments_df[
            self.payment_result_installments_df["payment_installments"]
            == selected_installment
        ]

        filtered_df = filtered_df.sort_values(by="total_payment", ascending=False)

        return filtered_df


# Export the instance
duckdb_setup = DuckDbSetup()
