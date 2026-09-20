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
