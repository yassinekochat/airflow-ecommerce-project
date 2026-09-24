# 🛒 E-Commerce ETL Pipeline with Apache Airflow

## 📌 Project Overview

This project demonstrates the implementation of a complete ETL (Extract, Transform, Load) pipeline using **Apache Airflow** on the Brazilian **Olist E-Commerce Dataset**.

The pipeline automates data validation, transformation, KPI computation, report generation, HTML dashboard creation, MongoDB storage, and category analysis using Dynamic Task Mapping.

The project is fully containerized with Docker and prepared for Continuous Integration using Jenkins.

---

# 🎯 Objectives

The main objectives of this project are:

- Build an automated ETL pipeline with Apache Airflow
- Process real-world e-commerce data
- Compute business KPIs
- Generate CSV and HTML reports
- Store analytical results into MongoDB
- Automate workflow execution
- Prepare the project for CI/CD using Jenkins

---

# 🛠 Technologies Used

| Technology | Purpose |
|---|---|
| Apache Airflow | Workflow orchestration |
| Python | Data processing |
| Pandas | Data manipulation |
| Docker | Containerization |
| Docker Compose | Multi-container deployment |
| MongoDB | NoSQL storage |
| Jenkins | Continuous Integration |
| Git & GitHub | Version control |
---

# 📂 Dataset

The project uses the **Brazilian Olist E-Commerce Dataset** available on Kaggle.

Files used:

- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_products_dataset.csv
- olist_customers_dataset.csv
- olist_order_reviews_dataset.csv
- olist_order_payments_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv

---

# 📁 Project Structure

```
airflow-ecommerce-project/

│
├── dags/
│   └── ecommerce_pipeline.py
│
├── data/
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_products_dataset.csv
│   └── ...
│
├── reports/
│   ├── kpi_report.csv
│   └── dashboard.html
│
├── plugins/
├── config/
├── logs/
├── docker/
├── scripts/
├── tests/
│
├── docker-compose.yml
├── requirements.txt
├── Jenkinsfile
└── README.md
```

---

# 🏗 Project Architecture

```
CSV Files
     │
     ▼
 FileSensor
     │
     ▼
Validate Orders
     │
     ▼
BranchPythonOperator
     │
     ▼
Load Data
     │
     ▼
Merge Data
     │
     ▼
Compute KPIs
     │
     ▼
Generate CSV Report
     │
     ▼
Generate HTML Dashboard
     │
     ▼
Store KPIs into MongoDB
     │
     ▼
Dynamic Task Mapping
     │
     ▼
Pipeline Summary
     │
     ▼
Finish
```

---

# ⚙ Airflow Pipeline

The DAG contains several tasks executed sequentially.

## 1. FileSensor

Waits until the Orders dataset is available.

---

## 2. Start Pipeline

Displays the pipeline start message.

---

## 3. Validate Orders

Checks that the Orders dataset is not empty.

---

## 4. BranchPythonOperator

Chooses the execution path according to the dataset size.

- Large Dataset
- Small Dataset

---

## 5. Load Data

Loads:

- Orders
- Order Items
- Products

---

## 6. Merge Data

Joins the datasets into a single dataframe.

---

## 7. Compute KPIs

Calculates:

- Total Orders
- Total Sales
- Average Price
- Top Selling Product
- Top Category

---

## 8. Generate CSV Report

Creates:

```
reports/kpi_report.csv
```

---

## 9. Generate HTML Dashboard

Creates:

```
reports/dashboard.html
```

The dashboard displays:

- Total Orders
- Total Sales
- Average Price
- Top Category

---

## 10. Save KPIs into MongoDB

Creates the database:

```
ecommerce_db
```

Collections:

- kpis
- categories

---

## 11. Dynamic Task Mapping

Airflow automatically creates one task for each product category.

Each task computes:

- Category name
- Number of products

---

## 12. Pipeline Summary

Displays the final execution summary.

---

## 13. Finish

Marks the successful completion of the pipeline.

---

# 📊 Computed KPIs

The pipeline calculates:

- Total Orders
- Total Sales
- Average Price
- Top Selling Product
- Top Product Category

---

# 💾 MongoDB

Database:

```
ecommerce_db
```

Collections:

```
kpis
categories
```

Example:

```
Total Orders
98666

Total Sales
13591643.70

Average Price
120.65

Top Category
cama_mesa_banho
```

---

# 📈 Dashboard

The HTML dashboard summarizes the KPIs in a user-friendly interface.

Generated file:

```
reports/dashboard.html
```

---

# 🐳 Docker

Containers used:

- Airflow Scheduler
- Airflow Worker
- Airflow Triggerer
- Airflow API Server
- PostgreSQL
- Redis
- MongoDB
- Jenkins

---

# 🚀 Installation

Clone the repository

```bash
git clone https://github.com/yassinekochat/airflow-ecommerce-project.git
```

Go to the project

```bash
cd airflow-ecommerce-project
```

Start Docker

```bash
docker compose up -d
```

Open Airflow

```
http://localhost:8080
```

Open Jenkins

```
http://localhost:8081
```

---

# ▶ Running the Pipeline

Activate the DAG:

```
ecommerce_pipeline
```

Run:

```
Trigger DAG
```

The generated reports will be available inside:

```
reports/
```

---

# 📷 Project Screenshots

Include screenshots of:

- Docker Desktop
- docker ps
- Airflow Graph View
- Airflow Grid View
- Task Logs
- MongoDB Collections
- HTML Dashboard
- Jenkins Build
- Generated CSV Report

---

# 🔄 Continuous Integration

The project is prepared for Jenkins.

The Jenkins pipeline performs:

- Project validation
- Report verification
- Dashboard verification
- Pipeline execution monitoring

---

# 🔮 Future Improvements

Possible improvements include:

- Plotly interactive dashboards
- Power BI integration
- PostgreSQL Data Warehouse
- Email notifications
- Automatic scheduling
- Unit testing
- Data quality monitoring
- GitHub Actions integration

---

# 👨‍💻 Author

**Yassine Kochat**

Master Data Engineer

Institut F2I

2026
