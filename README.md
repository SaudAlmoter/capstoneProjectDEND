# US Immigration data pipeline 
### Data Engineering Capstone Project

#### Project Summary
This is a Udacity Data Engineering Capstone project to showcase all the learning & skills that been acquired during the course of the nano-degree program.<br> This is an open-ended project and for this udacity has provided four datasets that includes US immigration 2016 data, airport codes, temperature and US demographic data.<br> If required, we can add more datasets to enrich information to suit our usecase which we like to analyze or present.<br> 

#### The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

#### The technologies used in this project:
*  1. Python
*  2. AWS S3 buckets
*  3. AWS Redshift
*  4. Spark
*  5. Airflow
  
#### How to use
First run the uploadFiles.py to upload files to the s3 buckets 'input'.<br>
After that run the eil.py that will Extract, Transfer, Load data to another s3 bucket 'output'.<br>
Then initaite the airlflow appache server and run the dag.<br>
