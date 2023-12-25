# ETL-API-Using-Python
Extract Random Jokes via API and Load Into BigQuery using Python


Here’s what I've done in this project:
-Got a random joke from the API
-Parsed it out into a pandas DataFrame
-Authenticate and connect to a BigQuery database
-Load the random joke data into a BigQuery table
-Deploy the script via schedule library to run every 2 minute, thus getting a new joke into our database every 2 minutes.

Google Cloud Bigquery Data Snapshot:
![random_joke_records_gc](https://github.com/aman7kh8an6/ETL-API-Using-Python/assets/42239133/b4cb37d9-6d5d-479d-8e54-c46175d876c7)
