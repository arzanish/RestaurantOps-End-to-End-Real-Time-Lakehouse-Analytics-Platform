-- Run in Databricks Workspace. To understand when to run , look at the README file.
INSERT INTO `01_bronze`.orders
SELECT * FROM `01_bronzw`.historical_orders;