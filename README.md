# Big_Data_Batch_Processing_Pipeline

A Data pipeline made using Apache Spark, Apache Airflow Deployed on GCP using Terraform.

## Description

### Objective
This project aims to simulate batch data processing activities for a retail chain where raw data produced by all the stores received in csv format goes through the cloud data pipeline to finally provide business insights on a Looker Studio Dashboard.

### Dataset

The dataset is sourced from Kaggle https://www.kaggle.com/datasets/berkayalan/retail-sales-data,
Contains data from a Turkish retail chain for years 2017 to 2019 consisting of 200 million records.

### Architecture
![Alt text](Images/FlowChart.png)

### Tools & Technologies

- Cloud - [**Google Cloud Platform**](https://cloud.google.com)
- Infrastructure as Code software - [**Terraform**](https://www.terraform.io)
- Orchestration - [**Airflow**](https://airflow.apache.org)
- Transformation and Batch Processing - [**Spark**](https://spark.apache.org/)
- Data Lake - [**Google Cloud Storage**](https://cloud.google.com/storage)
- Data Warehouse and Relational DB simulation - [**BigQuery**](https://cloud.google.com/bigquery)
- Data Visualization - [**Google Looker Studio**](https://datastudio.google.com/overview)
- Language - [**Python**](https://www.python.org)

### Final Result

![Alt text](Images/visualization.png)

This Dashboard shows top 10 revenue earning stores by their store id , Top 10 revenue earning cities and top 10 most sold products for the month of January and February of 2017

### Project Walkthrough

The cloud infrastructure for this project consists of :
* 2 Linux VM instances, one each for running Spark and Airflow
* 1 Google cloud storage bucket which will act as a data lake here
* 2 Google big query Schemas,  
    a.Staging which will act as a data warehouse    
    b.Production which will act as a relational DB storing processed data  

1.The data files in CSV format have been stored in the spark instance,  
```bash
Spark\upload_to_data_lake.py
```
This spark job picks the files , coverts them to partitioned parquet format and stores them to the google cloud storage bucket, this will be acting as the data lake for this project.  
![Alt text](Images/Screenshot(12).png)

2.The Parquet files are picked up from the google cloud storage by  

```bash
Spark\upload_to_data_warehouse.py
```
this spark job converts the files into dedicated bigquery tables :- sales-data, stores-data in the staging schema.

![Alt text](<Images/Screenshot (10).png>)  

3.The table data is read from the data warehouse using :-  

```bash
Spark\transform_upload.py  
```
this spark job also transforms the data for revenue and sales calculations and writes it to :  
a. revenue-data table in the production schema   
b. data split into monthly tables, which will be later used to make the comparison dashboard

![Alt text](Images/Screenshot(7).png)  


Execution:

Start the Airflow instance on the dedicated server using the command :

```bash
airflow standalone
```  
![Alt text](<Images/Screenshot (4).png>)

This starts the airflow web server and the same can be accesed at localhost:8080  
opening the DAG from the DAGs page, the graph view looks like the above picture.  

The DAG can be started using the button as highlighted in the picture below.  
The status of the first task changes to running,also notice the CPU usage of the spark instance, all cores being utilized fully, showing the multithreaded nature  

![Alt text](<Images/Screenshot (6).png>)

The Whole Pipeline completed in 8 minutes and 20 seconds as seen in the picture below:  

![Alt text](<Images/Screenshot (9).png>)  

