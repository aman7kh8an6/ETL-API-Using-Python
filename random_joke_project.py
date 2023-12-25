import requests
import schedule
import time as tm
import json
from datetime import datetime as dt
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

url = 'https://official-joke-api.appspot.com/random_joke'
key_path = '/Users/aman7/Downloads/stoked-mapper-409108-91b5298eaeb0.json'
project_id = 'stoked-mapper-409108'
data_set = 'joke_ddl'
table = 'random_jokes'
table_id="{}.{}.{}".format(project_id, data_set, table)

credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
def jobs():
    joke_data = requests.get(url).text
    joke_dict = json.loads(joke_data) 

    joke_id = joke_dict['id']
    joke_type = joke_dict['type']
    joke_setup = joke_dict['setup']
    joke_punchline = joke_dict['punchline']
    joke_datetime_extracted = dt.now()

    joke_df = pd.DataFrame(columns=['id', 'type', 'setup', 'punchline', 'datetime_extracted'])
    joke_df.loc[len(joke_df)] = [joke_id, joke_type, joke_setup, joke_punchline, joke_datetime_extracted]

    client = bigquery.Client(credentials=credentials, project=project_id)
    job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(
        joke_df, table_id, job_config=job_config
    )
    print('Updated record')
    job.result()  
    print(job.result())

schedule.every(2).minutes.do(jobs)

while True:
    schedule.run_pending()
    tm.sleep(1)