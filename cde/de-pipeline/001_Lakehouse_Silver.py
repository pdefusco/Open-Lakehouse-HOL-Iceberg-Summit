#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys, random, os, random
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS SILVER LAYER") \
    .getOrCreate()

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])

#---------------------------------------------------
#               LOAD TABLES FROM BRONZE LAYER
#---------------------------------------------------

airlinesDf = spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.airlines".format(username))

airportsDf = spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.airports".format(username))

flightsDf = spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.flights".format(username))

planesDf = spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.planes".format(username))

#---------------------------------------------------
#               LOAD DATA INTO SILVER LAYER
#---------------------------------------------------

# Airlines Table - Create empty table and populate it
spark.sql("""CREATE TABLE SPARK_CATALOG.{}_airlines.airlines_silver
                CODE STRING,
                DESCRIPTION STRING""".format(username))

airlinesDf\
    .writeTo("SPARK_CATALOG.{}_airlines.airlines_silver".format(username))\
    .using("iceberg")\
    .append()
#airlinesDf.write.format("iceberg").mode("append").save("SPARK_CATALOG.{}_airlines.airlines_silver".format(username))

# Airports and Planes Tables - Create from Dataframe
airlinesDf.writeTo("SPARK_CATALOG.{}_airlines.airports_silver".format(username))\
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

# Planes Table -
planesDf.writeTo("SPARK_CATALOG.{}_airlines.planes_silver".format(username))\
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .create()

# Flights Table - Create Table Selecting from Table
spark.sql("""CREATE TABLE SPARK_CATALOG.{0}_airlines.flights_silver
                USING iceberg
                AS SELECT *
                FROM SPARK_CATALOG.{0}_airlines.flights""".format(username))

#---------------------------------------------------
#               CREATE WAP BRANCH
#---------------------------------------------------

# CREATE TABLE BRANCH AND ADD DATA IN IT
spark.sql("ALTER TABLE SPARK_CATALOG.{0}_airlines.flights_silver CREATE BRANCH ingestion_branch".format(username))

batchDf = spark.sql("SELECT * FROM SPARK_CATALOG.airlines_csv.flights WHERE year = 2025 AND month = 1")

#trxBatchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0}".format(username))

batchDf.writeTo("SPARK_CATALOG.{}_airlines.planes_silver.ingestion_branch".format(username))\
    .using("iceberg") \
    .append()

#---------------------------------------------------
#               VALIDATE BATCH DATA IN BRANCH
#---------------------------------------------------

# validate the data quality of the sales data with great-expectations

geTrxBatchDf = SparkDFDataset(batchDf)

geTrxBatchDfValidation = geTrxBatchDf.expect_column_max_to_be_between(column="month", min_value=1, max_value=12)

print(f"VALIDATION RESULTS FOR BATCH DATA:\n{geTrxBatchDfValidation}\n")
assert geTrxBatchDfValidation.success, \
    "VALIDATION FOR TABLE UNSUCCESSFUL: COLUMN VALUES OUT OF RANGE."

### TRANSACTIONS FACT TABLE
branchDf = spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.planes_silver VERSION AS OF 'ingestion_branch';".format(username))

### TRX DF SCHEMA BEFORE CASTING
branchDf.printSchema()

print("COUNT OF NEW BATCH OF TRANSACTIONS")
print(branchDf.count())

#---------------------------------------------------
#               VALIDATE BATCH DATA IN BRANCH
#---------------------------------------------------

# validate the data quality of the sales data with great-expectations

geTrxBatchDf = SparkDFDataset(branchDf)

geTrxBatchDfValidation = geTrxBatchDf.expect_column_max_to_be_between(column="latitude", min_value=23, max_value=50)

print(f"VALIDATION RESULTS FOR TRANSACTION BATCH DATA:\n{geTrxBatchDfValidation}\n")
assert geTrxBatchDfValidation.success, \
    "VALIDATION FOR SALES TABLE UNSUCCESSFUL: FOUND DUPLICATES IN COLUMNS LIST."


#---------------------------------------------------
#               MERGE TRANSACTIONS WITH HIST
#---------------------------------------------------

### PRE-MERGE COUNTS BY TRANSACTION TYPE:
spark.sql("""SELECT COUNT(*) FROM SPARK_CATALOG.{}_airlines.planes_silver""".format(username)).show()

### APPEND OPERATION
#branchDf.write.format("iceberg").mode("append").save("SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0}".format(username))

### POST-MERGE COUNT:
#spark.sql("""SELECT COUNT(*) FROM SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0}""".format(username)).show()

### MERGE INGESTION BRANCH INTO MAIN TABLE BRANCH

#The cherrypick_snapshot procedure creates a new snapshot incorporating the changes from another snapshot in a metadata-only operation
#(no new datafiles are created). To run the cherrypick_snapshot procedure you need to provide two parameters:
#the name of the table you’re updating as well as the ID of the snapshot the table should be updated based on.
#This transaction will return the snapshot IDs before and after the cherry-pick operation as source_snapshot_id and current_snapshot_id.
#we will use the cherrypick operation to commit the changes to the table which were staged in the 'ing_branch' branch up until now.

# SHOW PAST BRANCH SNAPSHOT ID'S
spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.planes_silver.refs;".format(username)).show()

# SAVE THE SNAPSHOT ID CORRESPONDING TO THE CREATED BRANCH
branchSnapshotId = spark.sql("SELECT snapshot_id FROM SPARK_CATALOG.{}_airlines.planes_silver.refs WHERE NAME == 'ingestion_branch';".format(username)).collect()[0][0]

# USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
# THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
spark.sql("CALL spark_catalog.system.cherrypick_snapshot('SPARK_CATALOG.{}_airlines.planes_silver',{1})".format(username, branchSnapshotId))

# DROP BRANCH
#try:
#spark.sql("ALTER TABLE SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0} DROP BRANCH ingestion_branch".format(username))

# VALIDATE THE CHANGES
# THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.{}_airlines.planes_silver;".format(username)).show()
