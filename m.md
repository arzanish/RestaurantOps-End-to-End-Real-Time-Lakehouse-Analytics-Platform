# RestaurantOps

## End-to-End Real-Time Lakehouse Analytics Platform

📌 Architecture
📌 Business Problem
📌 Technology Stack
📌 Data Sources
📌 Ingestion Architecture
📌 Bronze Layer
📌 Silver Layer
📌 Gold Layer
📌 Spark Declarative Pipelines
📌 CDC with Lakeflow Connect
📌 Real-Time Event Streaming
📌 Databricks Workflow
📌 AI/BI Dashboards
📌 Key Engineering Challenges
📌 Project Screenshots
📌 Repository Structure
📌 How to Reproduce

RestaurantOps is an end-to-end Azure Databricks Lakehouse platform built for an Indian restaurant chain operating across five UAE locations.

The platform combines real-time order streaming through Azure Event Hubs, CDC-based ingestion from Azure SQL using Lakeflow Connect, historical data backfill, and a multi-layer Bronze/Silver/Gold architecture in Unity Catalog.

Real-time orders are consumed using the Kafka-compatible Event Hubs interface and Spark Structured Streaming through Spark Declarative Pipelines. Azure SQL operational data is ingested using Lakeflow Connect with an initial snapshot and ongoing CDC changes.

The Silver layer implements a dimensional data model consisting of fact tables for orders, order items and reviews, alongside dimensions for customers, restaurants and menu items.

The Gold layer uses materialized views and Databricks' incremental processing capabilities to produce business-ready datasets. Databricks Workflows orchestrate the ingestion and transformation pipelines.

The final solution exposes business insights through two Databricks AI/BI dashboards covering restaurant performance and customer review/sentiment analysis.

### EXECUTION FLOW :

1. Create **Event Hub**. Create a **SEND POLICY** ( Used by the Producer to authenticate and send events to Event Hub. It provides the required connection credentials with Send permission ) & a **LISTEN POLICY** (Used by Databricks/Consumer to authenticate and read events from Event Hub. It provides Listen permission)
2. Create an **Azure SQL DB**. Run the DDL statements. Here load data from the given CSV files. Enable **lakeflowSetupChangeTracking** & **lakeflowSetupChangeDataCapture** features using the UTILITY Script provided.
3. Start mimicking REAL-TIME data inflow using the **04_eventhub_orders.py** file. This will send data to EventHub.
4. Create a **Databricks WorkSpace**
5. Create 4 Schemas in Unity Catalog : 00_landing , 01_bronze , 02_silver , 03_gold
6. Create an **INGESTION PIPELINE (Pipeline_Ingestion_Bronze)** to ingest data from SQL SERVER to DB Workspace in the BRONZE LAYER. This creates tables : **historical_orders ; reviews.**.

Dump all the historical data (**historical_orders**) into the table : **orders** . Use the file : **Synthetic_Data/historical_dump.sql**

Create an **INGESTION PIPELINE (Pipeline_Ingestion_Silver)** to ingest data from SQL SERVER to DB Workspace in the SILVER LAYER. This creates tables : **dim_customers , dim_restaurants , dim_menu_items** 7. (Folder Pipelines) Now we create **ETL-Pipelines** in Databricks
PL : Pipeline_Ingestion_Eventhub
eventhub.py ==> Ingest data from EVENTHUB using KAFKA protocol.
Use SPARK DECLARATIVE PIPELINE. Create a Streaming table `01_bronze`.orders to store the ingested data.

PL : Pipeline_Transformation_Silver
fact_order_items.py ==> Use SPARK DECLARATIVE PIPELINE. Create `02_silver`.fact_order_items
fact_order.py ==> Use SPARK DECLARATIVE PIPELINE. Create `02_silver`. fact_orders
fact_reviews.sql ==> Use SDP. Create `02_silver`.fact_reviews

PL : Pipeline_Transform_Gold
d_360.py ==> Use SDP. Create 03_gold.customer_360
d_restaurant_reviews.py ==> Use SDP. Create 03_gold.d_restaurant_reviews
daily_sale_summary.py ==> Use SDP. Create 03_gold.d_sales_summary

8. Create a JOB and orchestrate the 3 ETL-Pipeline: Pipeline_Ingestion_Eventhub -> Pipeline_Transformation_Silver -> Pipeline_Transform_Gold

9.
