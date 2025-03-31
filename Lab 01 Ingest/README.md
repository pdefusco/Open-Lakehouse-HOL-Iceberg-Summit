# LAB 01: Ingest data using Apache NiFi

## Overview

In this lab, we will explore using **NiFi** to ingest new data into Iceberg tables. This process can take advantage of the ACID capabilities Iceberg provides, whether it is inserting new data into Iceberg tables or CDC transactions to provid real-time updates without rewriting entire datasets.

## Step-by-Step Guide

### Step 1: Setup

- This flow will load Flight data for Nov-2024 and Dec-2024.
- The source data is located in a **S3 bucket**.
- The target will be the `**<user0##>_airlines**` database and it will load serveral Iceberg table.  When completed you should see the following tables have been loaded: airlines, airports, planes, and flights
- This flow will be deployed using **Flow Designer** in a Cloudera AWS Public Cloud environment.


1. **Open Cloudera Data Flow**  
   - Download the flow definition [here](../../Assets/DataFlows/New_Flight_Data_to_Flights_Iceberg_Table.json).
   - Open the **DataHub NiFi** and create a new **Process Group**.
   - ![import_flow_definition.png](../../images/import_flow_definition.png)
   
2. **Go to Catalog**  
   - Browse to the JSON file on your disk. Once uploaded, you will see the following:  
   ![process_group.png](../../images/process_group.png)

3. **Create a New Draft of Iceberg-Summit-2025-data-flow**  
   - Double click on the Process Group to access the resulting Flow (with a few comments added to this screenshot):  
   ![nifi_flow.png](../../images/nifi_flow.png)

### Step 2: Getting the Flow to Execute

1. **Explore Data Flow**
   - Open CDP > Management Console > Environments > **your-environment**.
   - Click on the **Data Lake** tab.
   - Open Cloudera Manager by going to the Data Lake tab and clicking on the CM URL:  
   ![datalake_cm_url.png](../../images/datalake_cm_url.png)

2. **Start Test Session**
   - On the left nav, select **Clusters > Hive Metastore**:  
   ![cm_hive_metastore_config.png](../../images/cm_hive_metastore_config.png)
   - Click on **Actions > Download Client Configuration**:  
   ![cm_download_hms_client_config.png](../../images/cm_download_hms_client_config.png)

3. **Modify Parameters to use your User ID**
   - Once the client configuration is downloaded to your machine, unzip the file:  
   ![client_config_folder.png](../../images/client_config_folder.png)

4. **Start Cotroller All Services**  
   - Open `hive-site.xml` in a text editor and search for the key `hive.metastore.uris`.  
   - Copy the whole string between the `<value>` and `</value>` tags. This may contain 1 or more URIs separated by commas:  
   ![hive_metastore_uris.png](../../images/hive_metastore_uris.png)

5. **Start Airlines Processor and check output**
   - Use the following command to transfer files:  
   - Comment block for scp command:  
   ``` bash 
   scp <file>-site.xml <worker-pub-ip>:~
   ```

6. **Start All Processors and monitor progress**  
   - SSH into each worker node:
   ``` bash 
   ssh <worker-pub-ip>
   mkdir /tmp/<prefix>
   cp *.xml /tmp/<prefix>/
   sudo chown -R nifi:nifi /tmp/<prefix>
   ```

### Step 3: Verify Data Ingested Properly into Bronze Layer

1. **Open Cloudera Data Warehouse **  
   - Update the following parameters in NiFi:
     - **Access Key ID**: Your AWS access key to the CDP bucket where the new flight data (2009-2023) resides.
     - **Secret Key ID**: Your AWS secret key to the CDP bucket where the flight data resides.
     - **Workload User**: Your CDP workload user ID.
     - **Workload Password**: Your CDP workload password.
     - **Hadoop Configuration Resources**: Path to the XML files on the NiFi worker nodes, e.g., `/tmp/<prefix>/hive-site.xml,/tmp/<prefix>/core-site.xml,/tmp/<prefix>/hdfs-site.xml`.
     - **Database**: Should be `"<prefix>_airlines"`.
     - **Hive Metastore URI**: Use the URIs retrieved in the previous step.
   ![update_parameter_context.png](../../images/update_parameter_context.png)

2. **Open SQL Editor**  
   - Right-click the flow and select **Configure**:  
   ![configure_flow.png](../../images/configure_flow.png)
   - Go to the **Controller Services** tab:  
   ![controler_svcs_tab.png](../../images/controler_svcs_tab.png)
   - Enable all the Controller Services by clicking on the lightning bolt next to each Controller Service that has not yet been enabled:  
   ![enable_controler_svcs.png](../../images/enable_controler_svcs.png)

3. **Run some Queries to check results**  
   - If you see the following message, ensure your environment in NiFi worker nodes is correctly set up:  
   ![warning_nifi_not_setup_correctly.png](../../images/warning_nifi_not_setup_correctly.png)








Might be nice to have this be a processor that is investigated during monitoring
### Step 7: PutIceberg Processor Configuration

- **PutIceberg Processor Configuration**  
   - This processor is used to insert records into an Iceberg table. The records are read using the configured Record Reader, and the target Iceberg table is identified by the **Catalog Namespace** and **Table Name** properties.
   ![configure_puticeberg_processor.png](../../images/configure_puticeberg_processor.png)


## Summary

In this lab, we explored how to use **NiFi** for ingesting flight data from a public S3 bucket. You configured processors, set up controller services, and used Iceberg to manage the data. 


## Next Steps

Proceed to the next lab exercise: [CDC Debezium Data Flow](change_data_capture_debezium_DF.md).
