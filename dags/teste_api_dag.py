from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json
#import ssl

def fix_json_format(json_str):
    json_str = str(json_str)
    print(json_str)
    json_str = json_str.replace("'", '"')
    json_str = json_str.replace('\\', '\\\\')
    json_str = json_str.replace("True", "true")
    json_str = json_str.replace('None', '"None"')
    print(json_str)
    return json.loads(json_str)


def carrega_dados():
    print('Inicio carrega_dados')

    ultimo_resultado_carregado: int = 2558
    #ssl._create_default_https_context = ssl._create_unverified_context

    #json_valido = True
    #while json_valido:
    url = "https://servicebus2.caixa.gov.br/portaldeloterias/api/megasena/" + str(int(ultimo_resultado_carregado) + 1)
    response = requests.get(url, verify=False)
    result = json.loads(response.content)
    result = fix_json_format(result)
    print(result["numero"])
    print(ultimo_resultado_carregado)
    if result is not None and "exception" not in result and int(result["numero"])>ultimo_resultado_carregado:
        df = pd.json_normalize(result)
        print('LOG DF: ')
        print(df.describe)
        print(df.columns)
        ultimo_resultado_carregado = result["numero"]
    #else :
        #json_valido=False
        #break
    print(ultimo_resultado_carregado)    
    print('Fim carrega_dados')


with DAG('teste_api_dag', start_date = datetime(2023,1,1),
    schedule_interval="30 * * * *", catchup=False) as dag:

    carrega_dados = PythonOperator(
         task_id = 'carrega_dados',
         python_callable = carrega_dados
    )