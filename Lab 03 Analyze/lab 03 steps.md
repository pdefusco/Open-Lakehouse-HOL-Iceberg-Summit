# LAB 03: Analyze data using Apache Hive/Impala

## Overview

In this lab, we will explore using **NiFi** to ingest new data into Iceberg tables and simulate **Change Data Capture (CDC)** transactions. This process ensures that data is properly loaded into Iceberg tables and CDC transactions are accurately captured, providing real-time updates without rewriting entire datasets.

## Step-by-Step Guide

### Step 1: Setup

- This flow will load “newer” Flight data from the years 2009-2023 (not complete 2023).
- The source data is located in a **public S3 bucket**.
- The target will be the `**<prefix>_airlines.flights**` Iceberg table.
- This flow can also be deployed using **Flow Designer** as well—just follow this same approach.

> Note: At the time of writing this module, research is being conducted to acquire a Public AWS bucket for the new flights data and CSV files used to build the Data Lakehouse.

1. **Download the Flow Definition**  
   - Download the flow definition [here](../../Assets/DataFlows/New_Flight_Data_to_Flights_Iceberg_Table.json).
   - Open the **DataHub NiFi** and create a new **Process Group**.
   - ![import_flow_definition.png](../../images/import_flow_definition.png)
   
2. **Upload the JSON file**  
   - Browse to the JSON file on your disk. Once uploaded, you will see the following:  
   ![process_group.png](../../images/process_group.png)

3. **Access the Process Group**  
   - Double click on the Process Group to access the resulting Flow (with a few comments added to this screenshot):  
   ![nifi_flow.png](../../images/nifi_flow.png)

### Step 2: Getting the Flow to Execute

#### Prerequisite Steps

1. **Access Cloudera Manager**  
   - Open CDP > Management Console > Environments > **your-environment**.
   - Click on the **Data Lake** tab.
   - Open Cloudera Manager by going to the Data Lake tab and clicking on the CM URL:  
   ![datalake_cm_url.png](../../images/datalake_cm_url.png)

2. **Download Client Configuration**  
   - On the left nav, select **Clusters > Hive Metastore**:  
   ![cm_hive_metastore_config.png](../../images/cm_hive_metastore_config.png)
   - Click on **Actions > Download Client Configuration**:  
   ![cm_download_hms_client_config.png](../../images/cm_download_hms_client_config.png)

3. **Extract Configuration**  
   - Once the client configuration is downloaded to your machine, unzip the file:  
   ![client_config_folder.png](../../images/client_config_folder.png)

4. **Find the Hive Metastore URI**  
   - Open `hive-site.xml` in a text editor and search for the key `hive.metastore.uris`.  
   - Copy the whole string between the `<value>` and `</value>` tags. This may contain 1 or more URIs separated by commas:  
   ![hive_metastore_uris.png](../../images/hive_metastore_uris.png)

### Step 3: Transfer XML Files to NiFi Worker Nodes

1. **Open Command Prompt Terminal**  
   - Use the following command to transfer files:  
   - Comment block for scp command:  
   ``` bash 
   scp <file>-site.xml <worker-pub-ip>:~
   ```

2. **Access Worker Nodes via SSH**  
   - SSH into each worker node:
   ``` bash 
   ssh <worker-pub-ip>
   mkdir /tmp/<prefix>
   cp *.xml /tmp/<prefix>/
   sudo chown -R nifi:nifi /tmp/<prefix>
   ```

### Step 4: Modify NiFi UI Parameters

1. **Modify Parameters in NiFi**  
   - Update the following parameters in NiFi:
     - **Access Key ID**: Your AWS access key to the CDP bucket where the new flight data (2009-2023) resides.
     - **Secret Key ID**: Your AWS secret key to the CDP bucket where the flight data resides.
     - **Workload User**: Your CDP workload user ID.
     - **Workload Password**: Your CDP workload password.
     - **Hadoop Configuration Resources**: Path to the XML files on the NiFi worker nodes, e.g., `/tmp/<prefix>/hive-site.xml,/tmp/<prefix>/core-site.xml,/tmp/<prefix>/hdfs-site.xml`.
     - **Database**: Should be `"<prefix>_airlines"`.
     - **Hive Metastore URI**: Use the URIs retrieved in the previous step.
   ![update_parameter_context.png](../../images/update_parameter_context.png)

2. **Start Controller Services**  
   - Right-click the flow and select **Configure**:  
   ![configure_flow.png](../../images/configure_flow.png)
   - Go to the **Controller Services** tab:  
   ![controler_svcs_tab.png](../../images/controler_svcs_tab.png)
   - Enable all the Controller Services by clicking on the lightning bolt next to each Controller Service that has not yet been enabled:  
   ![enable_controler_svcs.png](../../images/enable_controler_svcs.png)

3. **Check for Errors**  
   - If you see the following message, ensure your environment in NiFi worker nodes is correctly set up:  
   ![warning_nifi_not_setup_correctly.png](../../images/warning_nifi_not_setup_correctly.png)

### Step 5: Run the Flow

- **Run all Processors**  
  - Start all the processors in the flow. Once the data has been processed, verify the data by querying the **flights** table in **HUE**.

### Step 6: ConvertRecord Processor Configuration

- **ConvertRecord Processor**  
   - This processor is used to convert the incoming CSV flight records for years 2009-2023 to match the Iceberg table's format, including removing unnecessary columns and converting data types as needed.
   ![configure_convert_record_processor.png](../../images/configure_convert_record_processor.png)

- **Record Reader/Writer Configuration**  
   - **Record Reader**: CSV Reader Controller Service. Set “Schema Access Strategy” to “Use ‘Schema Text’ Property”.  
   ![configure_csv_reader_controller_service.png](../../images/configure_csv_reader_controller_service.png)

   - Use this AVRO Schema for the new flights data (for more details, refer to the **Appendix** at the end of this submodule):
   ``` json
   {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "flights_new_data",
      "fields": [
        { "name": "year", "type": "int" },
        { "name": "month", "type": "int" },
        { "name": "dayofmonth", "type": "int" },
        { "name": "dayofweek", "type": "int" },
        { "name": "uniquecarrier", "type": "string" },
        { "name": "tailnum", "type": ["null","string"] },
        { "name": "flightnum", "type": ["null","int"] },
        { "name": "origin", "type": "string" },
        { "name": "dest", "type": "string" },
        { "name": "crsdeptime", "type": ["null","int"] },
        { "name": "deptime", "type": ["null","int"] },
        { "name": "depdelay", "type": ["null","double"] },
        { "name": "taxiout", "type": ["null","double"] },
        { "name": "taxiin", "type": ["null","double"] },
        { "name": "crsarrtime", "type": ["null","int"] },
        { "name": "arrtime", "type": ["null","int"] },
        { "name": "arrdelay", "type": ["null","double"] },
        { "name": "cancelled", "type": "double" },
        { "name": "cancellationcode", "type": ["null","string"] },
        { "name": "diverted", "type": "string" },
        { "name": "crselapsedtime", "type": ["null","double"] },
        { "name": "actualelapsedtime", "type": ["null","double"] },
        { "name": "airtime", "type": ["null","double"] },
        { "name": "distance", "type": ["null","double"] },
        { "name": "carrierdelay", "type": ["null","double"] },
        { "name": "weatherdelay", "type": ["null","double"] },
        { "name": "nasdelay", "type": ["null","double"] },
        { "name": "securitydelay", "type": ["null","double"] },
        { "name": "lateaircraftdelay", "type": ["null","double"] }
      ]
    }
   ```

### Step 7: PutIceberg Processor Configuration

- **PutIceberg Processor Configuration**  
   - This processor is used to insert records into an Iceberg table. The records are read using the configured Record Reader, and the target Iceberg table is identified by the **Catalog Namespace** and **Table Name** properties.
   ![configure_puticeberg_processor.png](../../images/configure_puticeberg_processor.png)

- **Catalog Service**  
   - Configure the **HiveCatalogService** using the **Hive Metastore URIs** acquired from the previous steps. The Hadoop Configuration Resources path should point to the location where the XML files were placed on the NiFi worker nodes.
   ![configure_hivecatalog_controller_service.png](../../images/configure_hivecatalog_controller_service.png)

### Step 8: Kerberos User Service

- **Kerberos User Service Configuration**  
   - This service is used to connect to the Hive Metastore Service by identifying the **Principal User** and **Password**. Make sure this is set correctly.
   ![configure_kerberos_pw_user_controller_service.png](../../images/configure_kerberos_pw_user_controller_service.png)

## Summary

In this submodule, we explored how to use **NiFi** for ingesting flight data from a public S3 bucket and simulate **Change Data Capture (CDC)**. We configured processors, set up controller services, and used Iceberg to manage the data. 

## Appendix

**Formatted AVRO Schema for incoming CSV Flights data:**
``` json
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "flights_new_data",
      "fields": [
        { "name": "year", "type": "int" },
        { "name": "month", "type": "int" },
        { "name": "dayofmonth", "type": "int" },
        { "name": "dayofweek", "type": "int" },
        { "name": "uniquecarrier", "type": "string" },
        { "name": "tailnum", "type": ["null","string"] },
        { "name": "flightnum", "type": ["null","int"] },
        { "name": "origin", "type": "string" },
        { "name": "dest", "type": "string" },
        { "name": "crsdeptime", "type": ["null","int"] },
        { "name": "deptime", "type": ["null","int"] },
        { "name": "depdelay", "type": ["null","double"] },
        { "name": "taxiout", "type": ["null","double"] },
        { "name": "taxiin", "type": ["null","double"] },
        { "name": "crsarrtime", "type": ["null","int"] },
        { "name": "arrtime", "type": ["null","int"] },
        { "name": "arrdelay", "type": ["null","double"] },
        { "name": "cancelled", "type": "double" },
        { "name": "cancellationcode", "type": ["null","string"] },
        { "name": "diverted", "type": "string" },
        { "name": "crselapsedtime", "type": ["null","double"] },
        { "name": "actualelapsedtime", "type": ["null","double"] },
        { "name": "airtime", "type": ["null","double"] },
        { "name": "distance", "type": ["null","double"] },
        { "name": "carrierdelay", "type": ["null","double"] },
        { "name": "weatherdelay", "type": ["null","double"] },
        { "name": "nasdelay", "type": ["null","double"] },
        { "name": "securitydelay", "type": ["null","double"] },
        { "name": "lateaircraftdelay", "type": ["null","double"] }
      ]
    }
```

**Formatted AVRO Schema for Iceberg table:**
``` json
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "flights_new_data",
      "fields": [
        { "name": "month", "type": "int" },
        { "name": "dayofmonth", "type": "int" },
        { "name": "dayofweek", "type": "int" },
        { "name": "deptime", "type": ["null","int"] },
        { "name": "crsdeptime", "type": ["null","int"] },
        { "name": "arrtime", "type": ["null","int"] },
        { "name": "crsarrtime", "type": ["null","int"] },
        { "name": "uniquecarrier", "type": "string" },
        { "name": "flightnum", "type": ["null","int"] },
        { "name": "tailnum", "type": ["null","string"] },
        { "name": "actualelapsedtime", "type": ["null","int"] },
        { "name": "crselapsedtime", "type": ["null","int"] },
        { "name": "airtime", "type": ["null","int"] },
        { "name": "arrdelay", "type": ["null","int"] },
        { "name": "depdelay", "type": ["null","int"] },
        { "name": "origin", "type": "string" },
        { "name": "dest", "type": "string" },
        { "name": "distance", "type": ["null","int"] },
        { "name": "taxiin", "type": ["null","int"] },
        { "name": "taxiout", "type": ["null","int"] },
        { "name": "cancelled", "type": "int" },
        { "name": "cancellationcode", "type": ["null","string"] },
        { "name": "diverted", "type": "string" },
        { "name": "carrierdelay", "type": ["null","int"] },
        { "name": "weatherdelay", "type": ["null","int"] },
        { "name": "nasdelay", "type": ["null","int"] },
        { "name": "securitydelay", "type": ["null","int"] },
        { "name": "lateaircraftdelay", "type": ["null","int"] },
        { "name": "year", "type": "int" }
      ]
    }
```

## Next Steps

Proceed to the next submodule: [CDC Debezium Data Flow](change_data_capture_debezium_DF.md).
