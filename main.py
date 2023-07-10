import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json

token_content = json.loads(os.environ['UPDATERTOKEN'])

with open('token.json', 'w') as file:
    json.dump(token_content, file)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery']
credentials = Credentials.from_service_account_file('token.json', scopes=scope)
client = gspread.authorize(credentials)


def sendDataToBigQuery(url, worksheet, destinationTable, cancelServices=False, renameColumns=False):
    sheet = client.open_by_url(url)
    databaseSheet = pd.DataFrame(sheet.worksheet(worksheet).get_all_records())

    if cancelServices:
        databaseSheet['isServicesCancellationSelected'] = True

    if renameColumns:
        databaseSheet = changeColumnName(destinationTable, databaseSheet)

    databaseSheet = databaseSheet.astype(str)
    databaseSheet.to_gbq(destination_table=destinationTable,
                         project_id='execution-tool-op', if_exists='replace', credentials=credentials)


def changeColumnName(destinationTable, df):
    new_names = {}
    if destinationTable == 'dashboards.tutelas':
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
        df = df.drop(columns=[''])
    elif destinationTable == 'dashboards.flightRefund':
        new_names = {
            'Data ': 'date',
            'Operador': 'operation_agent',
            'Pedido Hurb': 'OrderIds',
            'ID Operação': 'operation_id',
            'Solicitou cancelamento /Devolvido': 'isCancelled',
            'Ticket do cancelamento': 'cancellation_ticket',
            'Tipo de reembolso': 'cancellation_type',
            'Localizador': 'tracker',
            'Tipo de reserva': 'reservation_type',
            'Suplier': 'supplier',
            'Cia Aérea': 'airline',
            'Bilhete': 'air_ticket',
            'Status da Solicitação': 'airline_request_status',
            'Status OPV': 'opv_status',
            'Valor a reembolsar / reembolsado': 'refund_value',
            'Protocolo / BSP Link': 'bsp_link_protocol',
            'Observações': 'obs',
        }
    elif destinationTable == 'dashboards.SuspensionAction':
        new_names = {
            'Estabelecimento': 'establishment_name',
            'order_id':	'order_id',
            'operation_id':	'operation_id',
            'Reservas':	'reservation_code',
            'Checkin':	'checkin',
            'flight_tracker':	'flight_tracker',
            'Embarque':	'TravelStartDateBRT',
            'Retorno':	'ReturnDateBRT',
            'Hotel':	'hotel',
            'CiaAerea':	'airline',
            'Tutela':	'guardianship',
            'AereoCancelado':	'isAirTravelCancelled',
            'OperadorAereo':	'AirTravelOperator',
            'Ticket':	'ticket_number',
            'DataTratativa':	'DealDateBRT',
            'TerrestreCancelado':	'isTerrestrialCancelled',
            'OperadorTerrestre':	'TerrestrialOperator',
            'OBS':	'observation',
            'Tipo_Bloqueio':	'BlockType',
            'Reembolso':	'RefundValue',
            'obsReembolso':	'RefundObservation',
            'Multa':	'FeeValue',
            'obsMulta': 'FeeObservation'
        }
    return df.rename(columns=new_names)


sendDataToBigQuery('https://docs.google.com/spreadsheets/d/1ZI07h13_w_kNmFyPuVyVC7MksTxTZ0t4dlAib7suzFA/edit#gid=670717809',
                   'Geral', 'dashboards.tutelas', renameColumns=True)

sendDataToBigQuery('https://docs.google.com/spreadsheets/d/13sEnpKNVuOjrluLu94X7i5TRLAIJE_5M2zt5N5YGH-Y/edit#gid=0',
                   'Database', 'services_cancelations.orders')

sendDataToBigQuery('https://docs.google.com/spreadsheets/d/19gFL6vA90oSCrhBZcLE0P591CwH0clvLQJ9d7xrKmqE/edit#gid=0',
                   'Página1', 'dashboards.senacon_tutelas')

sendDataToBigQuery('https://docs.google.com/spreadsheets/d/1siIH4e16AOmXmk0ZfZo_pDPefPzXcilnmSWs7BQ9tBs/edit#gid=0',
                   'Reembolsos', 'dashboards.ReembolsosOP')

sendDataToBigQuery('https://docs.google.com/spreadsheets/d/1WQxAOmBRCJX3d_6Thra-UkdTaMLdjsL7f8NBwT6yilc/edit#gid=1241935875',
                   'Ação Reembolso_Relatório', 'dashboards.flightRefund', renameColumns=True)

sendDataToBigQuery('https://docs.google.com/spreadsheets/d/1h9z1_pg3jxLh0lleoPK0Ekhp8gObd-REZOvhAns9l3I/edit#gid=1623972009',
                   'Cancelamento Terrestre', 'dashboards.SuspensionAction', renameColumns=True)
