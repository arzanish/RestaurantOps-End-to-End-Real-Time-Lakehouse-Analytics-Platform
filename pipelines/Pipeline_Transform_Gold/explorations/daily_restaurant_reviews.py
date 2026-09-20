# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df_restaurants = spark.table("02_silver.dim_restaurants")
df_reviews = spark.table("02_silver.fact_reviews")

# COMMAND ----------



df_review_stats = (
    df_reviews
    .groupBy("restaurant_id")
    .agg(
        # Total reviews
        F.count("review_id").alias("total_reviews"),
        
        # Average rating
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        
        # Rating distribution
        F.sum(F.when(F.col("rating") == 5, 1).otherwise(0)).alias("rating_5_count"),
        F.sum(F.when(F.col("rating") == 4, 1).otherwise(0)).alias("rating_4_count"),
        F.sum(F.when(F.col("rating") == 3, 1).otherwise(0)).alias("rating_3_count"),
        F.sum(F.when(F.col("rating") == 2, 1).otherwise(0)).alias("rating_2_count"),
        F.sum(F.when(F.col("rating") == 1, 1).otherwise(0)).alias("rating_1_count"),
        
        # Sentiment counts
        F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_count"),
        F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_count"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_count"),
    )
)

display(df_review_stats)

# COMMAND ----------

df_restaurant_reviews = (
    df_restaurants.join(df_review_stats, on = "restaurant_id", how = "left")
    .select(
        "restaurant_id",
        col("name").alias("restaurant_name"),
    )
)

display(df_restaurant_reviews)