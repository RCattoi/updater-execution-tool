import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
from google.cloud import bigquery

token_content = json.loads(os.environ['UPDATERTOKEN'])

with open('token.json', 'w') as file:
    json.dump(token_content, file)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery']
credentials = Credentials.from_service_account_file('token.json', scopes=scope)
client = gspread.authorize(credentials)

user = bigquery.Client(project='execution-tool-op')


def query():
    return f'''
SELECT *
FROM `execution-tool-op.dashboards.ReembolsosOP` otj
'''


dataQuery = query()

dataQueryDF = user.query(dataQuery).to_dataframe()
print(dataQueryDF)
