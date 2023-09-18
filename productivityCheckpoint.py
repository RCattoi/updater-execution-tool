from google.oauth2.service_account import Credentials
import pandas as pd
import gspread
import json
import os


def productivityCheckpoint():
    token_content = json.loads(os.environ['UPDATERTOKEN'])

    with open('token.json', 'w') as file:
        json.dump(token_content, file)

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery']
    credentials = Credentials.from_service_account_file(
        'token.json', scopes=scope)
    client = gspread.authorize(credentials)

    sheet = client.open_by_url(
        "https://docs.google.com/spreadsheets/d/1emQFsP14Ttom4tiqpXzYP2rsCxIKZ-JYlK-RA4xwXvE/edit#gid=160585008")
    databaseSheet = pd.DataFrame(sheet.worksheet('Sheet1').get_all_records())
    databaseSheet = databaseSheet.astype(str)
    databaseSheet['DATA'] = pd.to_datetime(
        databaseSheet['DATA'], dayfirst=True)
    databaseSheet.to_gbq(destination_table='dashboards.productivityCheckpoint',
                         project_id='execution-tool-op', if_exists='replace')
