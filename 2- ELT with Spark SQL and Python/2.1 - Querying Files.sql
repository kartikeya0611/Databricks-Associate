-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Configure the dataset path as a notebook parameter using Widgets, enabling its use in SQL queries through the ${dataset_bookstore} placeholder.
-- MAGIC dbutils.widgets.text("dataset_bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

 SELECT *,
    _metadata.file_path source_file
  FROM json.`${dataset_bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`${dataset_bookstore}/books-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To create the **books_csv** table as an external table, first retrieve the default external location:
-- MAGIC - Navigate to the Catalog explorer in the left sidebar.
-- MAGIC - At the top, click the **External Data** button.
-- MAGIC - Copy the URL and replace the `<EXTERNAL-URL>` placeholder in the below two cell.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("external_location", '<EXTERNAL-URL>/external_storage')
-- MAGIC
-- MAGIC external_location = dbutils.widgets.get("external_location")
-- MAGIC dbutils.fs.cp(f"{dataset_bookstore}/books-csv", f"{external_location}/books-csv", recurse=True)

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${external_location}/books-csv"

-- COMMAND ----------

--SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

--DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{external_location}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{external_location}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{external_location}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

--SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

--REFRESH TABLE books_csv

-- COMMAND ----------

--SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset_bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset_bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TABLE books AS
SELECT * FROM read_files(
    '${dataset_bookstore}/books-csv/export_*.csv',
    format => 'csv',
    header => 'true',
    delimiter => ';');
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books
