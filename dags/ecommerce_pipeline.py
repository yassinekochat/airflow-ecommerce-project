from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule

from datetime import datetime
import pandas as pd
from pymongo import MongoClient


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce"],
) as dag:

    # =====================================================
    # FILE SENSOR
    # =====================================================

    wait_orders = FileSensor(
        task_id="wait_orders",
        filepath="/opt/airflow/data/olist_orders_dataset.csv",
        poke_interval=10,
        timeout=60 * 5,
        mode="poke",
    )

    # =====================================================
    # START
    # =====================================================

    @task
    def start():

        print("=" * 50)
        print("E-COMMERCE PIPELINE STARTED")
        print("=" * 50)

        return True

    # =====================================================
    # VALIDATION
    # =====================================================

    @task
    def validate_orders():

        orders = pd.read_csv(
            "/opt/airflow/data/olist_orders_dataset.csv"
        )

        print(f"Orders : {len(orders)}")

        if len(orders) == 0:
            raise Exception("Orders dataset is empty")

        return len(orders)

    # =====================================================
    # BRANCHING
    # =====================================================

    def choose_path(ti):

        nb = ti.xcom_pull(
            task_ids="validate_orders"
        )

        if nb > 50000:
            return "large_dataset"

        return "small_dataset"

    branch = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_path
    )

    large_dataset = EmptyOperator(
        task_id="large_dataset"
    )

    small_dataset = EmptyOperator(
        task_id="small_dataset"
    )

    # =====================================================
    # LOAD DATA
    # =====================================================

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_data():

        orders = pd.read_csv(
            "/opt/airflow/data/olist_orders_dataset.csv"
        )

        items = pd.read_csv(
            "/opt/airflow/data/olist_order_items_dataset.csv"
        )

        products = pd.read_csv(
            "/opt/airflow/data/olist_products_dataset.csv"
        )

        print(f"Orders : {len(orders)}")
        print(f"Items : {len(items)}")
        print(f"Products : {len(products)}")

        return True
    # =====================================================
    # MERGE DATA
    # =====================================================

    @task
    def merge_data():

        orders = pd.read_csv(
            "/opt/airflow/data/olist_orders_dataset.csv"
        )

        items = pd.read_csv(
            "/opt/airflow/data/olist_order_items_dataset.csv"
        )

        products = pd.read_csv(
            "/opt/airflow/data/olist_products_dataset.csv"
        )

        df = orders.merge(
            items,
            on="order_id"
        )

        df = df.merge(
            products,
            on="product_id"
        )

        print(f"Rows : {len(df)}")
        print(f"Columns : {len(df.columns)}")

        return True


    # =====================================================
    # COMPUTE KPIS
    # =====================================================

    @task
    def compute_kpis():

        orders = pd.read_csv(
            "/opt/airflow/data/olist_orders_dataset.csv"
        )

        items = pd.read_csv(
            "/opt/airflow/data/olist_order_items_dataset.csv"
        )

        products = pd.read_csv(
            "/opt/airflow/data/olist_products_dataset.csv"
        )

        df = orders.merge(items, on="order_id")

        df = df.merge(products, on="product_id")

        total_orders = df["order_id"].nunique()

        total_sales = round(
            df["price"].sum(),
            2
        )

        average_price = round(
            df["price"].mean(),
            2
        )

        top_product = (
            df.groupby("product_id")
            .size()
            .sort_values(ascending=False)
            .head(10)
        )

        top_category = (
            df.groupby("product_category_name")
            .size()
            .sort_values(ascending=False)
            .head(10)
        )

        print("=" * 40)
        print("KPI REPORT")
        print("=" * 40)

        print("Orders :", total_orders)
        print("Sales :", total_sales)
        print("Average Price :", average_price)

        print("\nTop Products")
        print(top_product)

        print("\nTop Categories")
        print(top_category)

        return {
            "orders": total_orders,
            "sales": total_sales,
            "average_price": average_price
        }
    # =====================================================
    # GENERATE REPORT
    # =====================================================

    @task
    def generate_report():

        orders = pd.read_csv(
            "/opt/airflow/data/olist_orders_dataset.csv"
        )

        items = pd.read_csv(
            "/opt/airflow/data/olist_order_items_dataset.csv"
        )

        products = pd.read_csv(
            "/opt/airflow/data/olist_products_dataset.csv"
        )

        df = orders.merge(items, on="order_id")
        df = df.merge(products, on="product_id")

        report = pd.DataFrame({
            "Metric": [
                "Total Orders",
                "Total Sales",
                "Average Price",
                "Top Category"
            ],
            "Value": [
                df["order_id"].nunique(),
                round(df["price"].sum(), 2),
                round(df["price"].mean(), 2),
                df["product_category_name"].value_counts().idxmax()
            ]
        })

        report.to_csv(
            "/opt/airflow/reports/kpi_report.csv",
            index=False
        )

        report.to_json(
            "/opt/airflow/reports/kpi_report.json",
            orient="records",
            indent=4
        )

        print(report)

        return "/opt/airflow/reports/kpi_report.csv"

    @task
    def generate_dashboard():

        report = pd.read_csv(
            "/opt/airflow/reports/kpi_report.csv"
        )

        html = f"""
        <!DOCTYPE html>
        <html>

        <head>

            <title>E-Commerce Dashboard</title>

            <style>

                body{{
                    font-family:Arial;
                    background:#f4f6f9;
                    margin:40px;
                }}

                h1{{
                    text-align:center;
                    color:#2c3e50;
                }}

                .container{{
                    display:flex;
                    flex-wrap:wrap;
                    justify-content:center;
                    gap:20px;
                    margin-top:40px;
                }}

                .card{{
                    background:white;
                    width:250px;
                    padding:20px;
                    border-radius:10px;
                    box-shadow:0 2px 10px rgba(0,0,0,.15);
                    text-align:center;
                }}

                .metric{{
                    font-size:18px;
                    color:#555;
                }}

                .value{{
                    font-size:32px;
                    color:#0078D4;
                    font-weight:bold;
                    margin-top:10px;
                }}

            </style>

        </head>

        <body>

            <h1>E-Commerce KPI Dashboard</h1>

            <div class="container">
        """

        for _, row in report.iterrows():

            html += f"""

            <div class="card">

                <div class="metric">
                    {row['Metric']}
                </div>

                <div class="value">
                    {row['Value']}
                </div>

            </div>

            """

        html += """

            </div>

        </body>

        </html>

        """

        with open(
            "/opt/airflow/reports/dashboard.html",
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        print("Dashboard HTML generated")

        return "/opt/airflow/reports/dashboard.html" 
    # =====================================================
    # SAVE KPI TO MONGODB
    # =====================================================

    @task
    def save_to_mongodb():

        report = pd.read_csv(
            "/opt/airflow/reports/kpi_report.csv"
        )

        client = MongoClient(
            "mongodb://mongodb:27017"
        )

        db = client["ecommerce_db"]

        collection = db["kpis"]

        collection.delete_many({})

        collection.insert_many(
            report.to_dict("records")
        )

        print(
            f"KPI inserted : {collection.count_documents({})}"
        )

        return collection.count_documents({})


    # =====================================================
    # GET CATEGORIES
    # =====================================================

    @task
    def get_categories():

        products = pd.read_csv(
            "/opt/airflow/data/olist_products_dataset.csv"
        )

        categories = (
            products["product_category_name"]
            .dropna()
            .unique()
            .tolist()
        )

        return categories[:10]


    # =====================================================
    # ANALYSE CATEGORY
    # =====================================================

    @task
    def analyse_category(category):

        products = pd.read_csv(
            "/opt/airflow/data/olist_products_dataset.csv"
        )

        nb_products = len(
            products[
                products["product_category_name"] == category
            ]
        )

        result = {
            "category": category,
            "nb_products": nb_products
        }

        print(result)

        return result


    # =====================================================
    # SAVE CATEGORY RESULTS
    # =====================================================

    @task
    def save_categories(results):

        client = MongoClient(
            "mongodb://mongodb:27017"
        )

        db = client["ecommerce_db"]

        collection = db["categories"]

        collection.delete_many({})

        collection.insert_many(results)

        print(
            f"Categories inserted : {collection.count_documents({})}"
        )

        return collection.count_documents({})
    # =====================================================
    # PIPELINE SUMMARY
    # =====================================================

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def pipeline_summary():

        print("=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)

        print("Orders loaded")
        print("Merge completed")
        print("KPI generated")
        print("CSV report generated")
        print("JSON report generated")
        print("MongoDB updated")
        print("Category analysis completed")

        return True


    # =====================================================
    # FINISH
    # =====================================================

    @task
    def finish():

        print("=" * 60)
        print("E-COMMERCE PIPELINE FINISHED SUCCESSFULLY")
        print("=" * 60)


    # =====================================================
    # EXECUTION
    # =====================================================

    start_task = start()

    validation = validate_orders()

    load = load_data()

    merge = merge_data()

    kpis = compute_kpis()

    report = generate_report()
    dashboard = generate_dashboard()
    mongo = save_to_mongodb()

    categories = get_categories()

    analysis = analyse_category.expand(
        category=categories
    )

    save_categories_task = save_categories(analysis)

    summary = pipeline_summary()

    end = finish()


    # =====================================================
    # DEPENDENCIES
    # =====================================================

    wait_orders >> start_task

    start_task >> validation

    validation >> branch

    branch >> large_dataset
    branch >> small_dataset

    large_dataset >> load
    small_dataset >> load

    load >> merge
    merge >> kpis
    kpis >> report
    report >> dashboard

    dashboard >> mongo
    mongo >> categories
    categories >> analysis
    analysis >> save_categories_task
    save_categories_task >> summary
    summary >> end