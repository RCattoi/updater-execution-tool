import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
from google.cloud import bigquery


def zendeskProductivity():
  def query():
    return f'''
  SELECT *
  FROM `execution-tool-op.dashboards.ReembolsosOP` otj
  '''

  dataQuery = query()
  user = bigquery.Client(project='execution-tool-op')
  dataQueryDF = user.query(dataQuery).to_dataframe()



  token_content = json.loads(os.environ['UPDATERTOKEN'])

  with open('token.json', 'w') as file:
      json.dump(token_content, file)

  scope = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery']
  credentials = Credentials.from_service_account_file('token.json', scopes=scope)
  gc = gspread.authorize(credentials)

    
  sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1HnynDTuKWzU7ITvVTLTO6PzU-QVNuZ4fuU0LZ0z0c7E/edit#gid=1387845271")
  databaseSheet = pd.DataFrame(sheet.worksheet('productivity').get_all_records())
  print(databaseSheet)
  return databaseSheet
