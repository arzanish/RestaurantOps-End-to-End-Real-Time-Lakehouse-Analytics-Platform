# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df_orders = spark.table("02_silver.fact_orders")

df_order_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("total_amount").alias("lifetime_spend"),
        round(avg("total_amount"),2).alias("avg_order_value"),
        max("order_date").alias("last_order_date")
    )
    .withColumn(
        "loyalty_tier",
        when(col("lifetime_spend") >=5000 , "Platinum")
        .when(col("lifetime_spend") >=2000 , "Gold")
        .when(col("lifetime_spend") >=1000 , "Silver")
        .otherwise("Bronze")
        )
)

display(df_order_stats)

# COMMAND ----------

df_reviews = spark.table("02_silver.fact_reviews")
df_reviews_stats = (
    df_reviews
    .groupBy("customer_id")
    .agg(
        countDistinct("review_id").alias("total_reviews"),
        round(avg("rating"),2).alias("avg_rating_given")
    )
)

display(df_reviews_stats)

# COMMAND ----------

df_restaurants = spark.table("02_silver.dim_restaurants")
df_fav_restaurant = (
    df_orders.join(df_restaurants, on ="restaurant_id",how="inner")
    .groupBy("customer_id","name")
    .agg(
        count("order_id").alias("order_ct")
    )
    .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("order_ct").desc())))
    .filter(col("rn")==1)
    .drop(col("rn"))
    .select("customer_id",col("name").alias("restaurant_name"))
    
)

display(df_fav_restaurant)

# COMMAND ----------

df_fact_order_items = spark.table("02_silver.fact_order_items")

df_fav_item = (
    df_orders.join(df_fact_order_items , on = "order_id")
    .groupBy("customer_id","item_name")
    .agg(
        sum("quantity").alias("item_qty")
    )
    .withColumn("rn",row_number().over(Window.partitionBy("customer_id").orderBy(col("item_qty").desc()))
                )
    .filter(col("rn")==1)
    .drop(col("rn"))
    .select("customer_id",col("item_name").alias("favourite_item"))
    .orderBy("customer_id","item_name",desc("item_qty"))
)

display(df_fav_item)

# COMMAND ----------

df_customers = spark.table("02_silver.dim_customers")
df_c360 = (
    df_customers
        .join(df_order_stats,on="customer_id",how="left")
        .join(df_reviews_stats,on="customer_id",how="left")
        .join(df_fav_restaurant,on="customer_id",how="left")
        .join(df_fav_item,on="customer_id",how="left")
        .select(
            "customer_id",
            col("name").alias("customer_name"),
            "email",
            "city",
            "join_date",

            # order stats
            "loyalty_tier",
            coalesce("total_orders",lit(0)).cast("decimal(10,2)").alias("total_orders"),
            coalesce("lifetime_spend",lit(0)).cast("decimal(10,2)").alias("lifetime_spend"),
            coalesce("avg_order_value",lit(0)).cast("decimal(10,2)").alias("avg_order_value"),
            "last_order_date",
            

            # review stats
            coalesce("total_reviews", lit(0)).cast("decimal(10,2)").alias("total_reviews"),
            coalesce("avg_rating_given",lit(0)).cast("decimal(10,2)").alias("avg_rating_given"),


            # favourite item
            "restaurant_name",
            "favourite_item",
            when(col("lifetime_spend") >= 5000,True).otherwise(False).alias("is_vip")
                )
)

display(df_c360)

# COMMAND ----------

