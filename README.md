# Airflow_ETL_Pipeline

## **Overview**
This project demonstrates an end-to-end data engineering pipeline designed to automate the **ETL (Extract, Transform, Load)** process using industry-leading tools: **AWS**, **DBT**, **Snowflake**, and **Apache Airflow**. The pipeline ensures efficient, scalable, and automated data transformations to enable advanced analytics and business intelligence.


---

## **Tech Stack**
- **AWS S3**: Secure and scalable storage for raw data.  
- **Snowflake**: A powerful data warehouse for storing and querying large datasets.  
- **DBT**: A transformation tool that enables data modeling and testing in Snowflake.  
- **Apache Airflow**: A workflow orchestration tool to manage the ETL process.  
- **Slack**: Integrated for real-time alerting and notifications.  

---

## **Pipeline Workflow**
1. **Data Ingestion**:  
   - Data is extracted from a source system and uploaded to an AWS S3 bucket.  

2. **Data Loading**:  
   - Raw data is ingested into Snowflake using Airflow tasks.  

3. **Data Transformation**:  
   - DBT is used to model and transform raw data into analytics-ready datasets.  

4. **Automation and Orchestration**:  
   - Airflow orchestrates the entire workflow, including dependencies, retries, and notifications.  

5. **Monitoring and Alerts**:  
   - Slack alerts provide updates on task status, including pipeline failures and completions.  

-


