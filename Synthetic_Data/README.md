![](../diagrams/synthetic_data.png)


STEP1 : **data** Folder
1. Create an AZURE SQL SERVER DATABASE and use the script azuresqldatabase_setup.sql  to create the tables : historical_orders , reviews , customers , menu_items , restaurants.

2. Run the utility_script.sql (download the latest one from the DATABRICKS website) in the SQL Server.. This will help in achieving CDC (Change Data Capture) and CT (Change Tracking) .

3. Run the commands given in the azuresqldatabase_setup.sql and also alter the commands with respect to the Database USERNAME and PASSWORD.

4. gold_schemas.md and silver_schemas.md are given for reference. We will use this data in the Databricks Platform when creating these tables.
