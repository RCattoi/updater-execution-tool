import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os


UPDATERTOKEN = os.environ['UPDATERTOKEN']

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file(UPDATERTOKEN, scopes=scope)
client = gspread.authorize(credentials)


def sendDataToBigQuery(url, worksheet, destinationTable, cancelServices=False, renameColumns=False):
    sheet = client.open_by_url(url)
    databaseSheet = pd.DataFrame(sheet.worksheet(worksheet).get_all_records())

    if cancelServices:
        databaseSheet['isServicesCancellationSelected'] = True

    if renameColumns:
        databaseSheet = databaseSheet.rename(columns=renameColumns)

    databaseSheet = databaseSheet.drop(columns=[''])
    databaseSheet = databaseSheet.astype(str)
    databaseSheet.to_gbq(destination_table=destinationTable,
                         project_id='execution-tool-op', if_exists='replace')


new_names = {
    'DATA FATAL ': 'data_fatal',
    'PEDIDOS': 'order_id',
    'DIAS DE ATRASO': 'dias_atraso',
    'VALOR DA MULTA': 'valor_multa',
    'TIPO DE MULTA': 'tipo_multa',
    'MULTA ATUAL': 'multa_atual',
    'VALOR DO AÉREO': 'valor_aereo',
    'VALOR DO AÉREO - Estimado (METABASE)': 'valor_aereo',
    'OBSERVAÇÃO': 'obs',
    'PEDIDO ÚNICO': 'pedido_uncio',
    'multa acumulada': 'multa_acumulada',

}
sendDataToBigQuery('https://docs.google.com/spreadsheets/d/1ZI07h13_w_kNmFyPuVyVC7MksTxTZ0t4dlAib7suzFA/edit#gid=670717809',
                   'Geral', 'dashboards.tutelas', renameColumns=new_names)
