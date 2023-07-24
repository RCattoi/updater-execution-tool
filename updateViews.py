import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
import numpy as np
from columnParser import *

token_content = json.loads(os.environ['UPDATERTOKEN'])

with open('token.json', 'w') as file:
    json.dump(token_content, file)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery']
credentials = Credentials.from_service_account_file('token.json', scopes=scope)
client = gspread.authorize(credentials)

f = open('queries.json')
queries = json.load(f)
tableNames = list(queries.keys())
sqlCodes = list(queries.values())

for tn in range(len(tableNames)):
    query_data = pd.io.gbq.read_gbq(query=sqlCodes[tn], project_id="bi-data-science", dialect='standard')
    query_data.to_gbq(destination_table='views.' + tableNames[tn], project_id='execution-tool-op', if_exists='replace')
