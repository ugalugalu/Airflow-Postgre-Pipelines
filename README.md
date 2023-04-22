This is a project on Ochestrating ETL workflow on Airflow

BRIEF DOCUMENTATION

3 Best Practices

1.	The use of default parameters Dictionary:  Default args dictionary was used to simplify and ensure that arguments are standard for all the tasks.
2.	Name of tasks and Dag’s: The names were straight forward for ease of troubleshooting and maintenance.
3.	The number of tasks used were precise to tasks at Hand: The tasks were precise to Extraction, Transformation and Loading of Data to the Database


Recommendations to Run the Pipeline in Cloud Providers

1.	The solution can be deployed to cloud providers like Google cloud to ensure stability and performance by having more executors. On my local machine am using only one executor which is not scalable.
2.	Change the metadata Database to Postgre SQL : On my local machine am using SQLite to store metadata. This can be challenging when scalability is required.
Screen Shots of Pipeline in Airflow

View of DAGs in Airflow

 
<img width="452" alt="image" src="https://user-images.githubusercontent.com/54645939/233794071-96a699b4-4463-4639-8a51-958994cee139.png">
<img width="452" alt="image" src="https://user-images.githubusercontent.com/54645939/233794082-cc99b924-4e78-4f0b-a97b-87808f661ff7.png">




 

Dags running in Airflow CLI

 
<img width="452" alt="image" src="https://user-images.githubusercontent.com/54645939/233794092-3243e6d8-9b19-4505-af13-c4a39159aac4.png">

 
