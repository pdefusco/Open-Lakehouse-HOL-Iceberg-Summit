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
import sys, random, os, json, random

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS GOLD LAYER") \
    .getOrCreate()

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])

#---------------------------------------------------
#               ICEBERG INCREMENTAL READ
#---------------------------------------------------

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.flights_silver.history".format(username)).show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.flights_silver.snapshots".format(username)).show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE
snapshots_df = spark.sql("SELECT * FROM SPARK_CATALOG.{}_airlines.flights_silver.snapshots;".format(username))

# SNAPSHOTS
snapshots_df.show()

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
#second_snapshot = snapshots_df.select("snapshot_id").collect()[1][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

incReadDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.{}_airlines.flights_silver".format(username))

print("Incremental Report:")
incReadDf.show()

#-----------------------------------------------------
#               BUILD GOLD LAYER TABLES
#-----------------------------------------------------

# Flights Table - Create Table Selecting from Table
spark.sql("""CREATE TABLE SPARK_CATALOG.{0}_airlines.flights_planes_reduced_gold
                USING iceberg
                AS SELECT F.tailnum, P.model, P.status, F.month, F.dayofmonth, F.flightnum
                FROM SPARK_CATALOG.{0}_airlines.flights F
                INNER JOIN SPARK_CATALOG.{0}_airlines.planes P
                ON F.tailnum = P.tailnum""".format(username))

# Flights Table - Create Table Selecting from Table
spark.sql("""CREATE TABLE SPARK_CATALOG.{0}_airlines.flights_diverted_gold
                USING iceberg
                AS SELECT *
                FROM SPARK_CATALOG.{0}_airlines.flights F
                WHERE F.diverted == 1
                """.format(username))

spark.stop()
