# Capstone_google_composer_project
In this project we are generating oltp schema which consist of four tables and then transfer data to bigquery using google composer star schema
1. Create sql instance on GCP using postgreql
2. Create a oltp schema using oltp postgresql
3. Generate fake data using faker library which can be done using OLTP-fake-library.ipynb
4. insert data into created oltp schema using gcloud cli or using third party software
5. Create star schema defined in table using reusablestarschema.txt
6. create composer environmen t which will be used to transfer data from postfresql to bigquery
7. dag_file.py consist of the required code to call task1.py using composer airflow
8. task1 file contained the ETL pipeline build to extract data from postgresql using posgr2 python library then transform it into 6 starschema table then load them into bigquery using bigquery client python library
9. then again generate 5000 records and populate them into oltp and run ETL
10. task1 will take care of incremental load
11. go to bigquery and perform analysis on the transfered data.
