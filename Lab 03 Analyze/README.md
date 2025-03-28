# Module 13 - Data Ingestion and Change Data Capture (CDC)

## Overview

This module focuses on **Data Ingestion** and **Change Data Capture (CDC)** techniques for managing data in Iceberg tables on the Cloudera Data Platform (CDP). It covers using NiFi, Debezium, GoldenGate, and custom flows for ingesting data and simulating CDC transactions in a variety of scenarios.

### Key Concepts of Data Ingestion and Why CDC is Important

1. **Data Ingestion**  
   - Ingesting data from external sources into Iceberg tables, ensuring that it is properly formatted and stored for further analytics.

2. **Change Data Capture (CDC)**  
   - Capturing changes in data (inserts, updates, and deletes) and applying them to Iceberg tables while maintaining the data's consistency and integrity.

3. **Why CDC is Important**  
   - **Efficient updates**
     - Capture changes as they happen and propagate them efficiently to downstream systems.
   - **Preservation of historical data**
     - CDC allows tracking of how data has evolved over time.
   - **Optimized storage**
     - Only changes are captured and propagated, optimizing storage and computation resources.

## Prerequisites

Before working with **Data Ingestion** and **CDC** in this module, ensure you have the following:

1. **Cloud CDP Environment Setup**  
   - Ensure your Cloud CDP Environment, PvC Data Service, or CDP Base Cluster is fully set up. This document does not cover installation.

2. **Iceberg Table Setup**  
   - You should have Iceberg tables set up with the required schema, including the `flights` and `airlines` tables used for this use case.
   
3. **Data Lakehouse**  
   - Familiarity with the structure and schema of a **Data Lakehouse** powered by Iceberg.

4. **Access to NiFi**

   Depending on the submodule you're working on, ensure you have the appropriate NiFi access:
   
   - **Submodule 01**
   
      - Requires access to **NiFi via the Flow Management Data Hub** for loading flight data.
   
   - **Submodules 02, 03, and 04**
   
      - Requires access to **NiFi via the DataFlow Data Service** for CDC simulation with Debezium, GoldenGate, and custom flows.

> **Note:** Iceberg may or may not be the optimal solution for fast-changing data (such as millions of updates or deletes per second). In such cases, consider alternatives like **Kudu** for high-frequency data changes.

---

### Methods Covered in This Module

#### 1. NiFi Data Ingestion and CDC Flow

This method covers using NiFi to ingest data into Iceberg tables and simulate CDC transactions using Iceberg processors, available in a **Flow Management Data Hub**.

#### 2. CDC with Debezium

This method covers how to use Debezium to simulate CDC transactions for Iceberg tables, including updates, deletes, and inserts, using the **DataFlow Data Service**.

#### 3. CDC with GoldenGate

This method covers how to handle CDC transactions using GoldenGate, simulating the ingestion of updates, deletes, and inserts for Iceberg tables, using the **DataFlow Data Service**.

#### 4. Custom CDC Data Flow

This method demonstrates a custom CDC data flow using Iceberg, enabling a flexible approach to ingesting CDC transactions from custom data sources, using the **DataFlow Data Service**.

---

### Key Takeaways

- CDC allows efficient and real-time data changes to be captured and applied without rewriting entire tables.
- Iceberg tables support in-place data updates, ensuring data integrity across inserts, updates, and deletes.
- Each method offers different levels of flexibility depending on the use case and data source (NiFi, Debezium, GoldenGate, or custom).

> **Note:** Remember to replace `${prefix}` with your chosen value (e.g., your User ID) throughout the process.

---

## Submodules

Choose one of the following submodules to get started:

- `01` [NiFi Data Ingestion and CDC Flow](load_new_data_to_flights_DF.md) - **Requires Flow Management Data Hub**
- `02` [CDC Debezium Data Flow](change_data_capture_debezium_DF.md) - **Requires DataFlow Data Service**
- `03` [CDC GoldenGate Data Flow](change_data_capture_goldengate_DF.md) - **Requires DataFlow Data Service**
- `04` [CDC Custom Data Flow](change_data_capture_custom_DF.md) - **Requires DataFlow Data Service**
