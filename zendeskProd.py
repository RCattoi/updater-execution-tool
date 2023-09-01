import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
from google.cloud import bigquery


def zendeskProductivity():
    user = bigquery.Client(project='execution-tool-op')

    def query():
        return f'''
  SELECT *
  FROM `execution-tool-op.dashboards.ReembolsosOP` otj
  '''
    dataQuery = query()
    dataQueryDF = user.query(dataQuery).to_dataframe()
    return dataQueryDF