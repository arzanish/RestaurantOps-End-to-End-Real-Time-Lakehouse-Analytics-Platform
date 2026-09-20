![](../diagrams/synthetic_data.png)


### STEP1 : SQL Folder
1. Create an AZURE SQL SERVER DATABASE and use the script azuresqldatabase_setup.sql  to create the tables : historical_orders , reviews , customers , menu_items , restaurants.

2. Run the utility_script.sql (download the latest one from the DATABRICKS website) in the SQL Server.. This will help in achieving CDC (Change Data Capture) and CT (Change Tracking) .

3. Run the commands given in the azuresqldatabase_setup.sql and also alter the commands with respect to the Database USERNAME and PASSWORD.

4. gold_schemas.md and silver_schemas.md are given for reference. We will use this data in the Databricks Platform when creating these tables.


### STEP2 : DATA Folder
1. Use a Tool like DATAGRIP to load data onto the tables in the AZURE SQL SERVER DB. Or simply use the AZURE SQL DB itself to do so .


### STEP3 : CREATE EVENTHUB
1.  Create an AZURE EVENTHUB .
2.  Use the file **04_eventhub_orders.py** to generate streaming data and send to the AZURE EVENT HUB. Make sure to put in the credentials of the EVENT HUB in a **.env** File and use it.
