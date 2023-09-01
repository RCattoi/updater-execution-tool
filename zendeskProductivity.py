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
  FROM `execution-tool-op.dashboards.productivityOpZendesk`
  '''

    dataQuery = query()
    user = bigquery.Client(project='execution-tool-op')
    dataQueryDF = user.query(dataQuery).to_dataframe()

    token_content = json.loads(os.environ['UPDATERTOKEN'])

    with open('token.json', 'w') as file:
        json.dump(token_content, file)

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery']
    credentials = Credentials.from_service_account_file(
        'token.json', scopes=scope)
    gc = gspread.authorize(credentials)

    sheet = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1HnynDTuKWzU7ITvVTLTO6PzU-QVNuZ4fuU0LZ0z0c7E/edit#gid=1387845271")
    databaseSheet = pd.DataFrame(
        sheet.worksheet('productivity').get_all_records())

    concatedDF = pd.concat([databaseSheet, dataQueryDF], ignore_index=True)
    concatedDF = concatedDF.astype(str)
    concatedDF = concatedDF.drop_duplicates()

    if(len(concatedDF) and len(dataQueryDF) and len(databaseSheet)):
        concatedDF.to_gbq(destination_table='dashboards.productivityOpZendesk',
                          project_id='execution-tool-op', if_exists='replace')

        sheet = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1HnynDTuKWzU7ITvVTLTO6PzU-QVNuZ4fuU0LZ0z0c7E/edit#gid=1387845271")
        sheet.values_clear("productivity!A2:D")

        print("Update for Zendeskproductivity Table completed")
