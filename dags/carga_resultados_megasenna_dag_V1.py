import json
from json import JSONEncoder
import pandas as pd
import numpy as np
import requests
import os
import logging
import openpyxl
from openpyxl import Workbook
from datetime import datetime
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

with DAG(
    dag_id='carga_resultados_megasenna_dag_V1', 
    start_date = datetime(2023,1,1),
    schedule_interval="30 * * * *",
    #schedule_interval="@daily", 
    catchup=False
) as dag:

    def fix_json_format(json_str):
        json_str2 = str(json_str)
        print(json_str2)
        json_str2 = json_str2.replace("'", '"')
        json_str2 = json_str2.replace("\"RIO GRANDE DO NORTE\"", "'RIO GRANDE DO NORTE'")  
        json_str2 = json_str2.replace("D\"OESTE", "D'OESTE") 
        json_str2 = json_str2.replace("\"A C U M U L A D A\"", "'A C U M U L A D A'") 
        json_str2 = json_str2.replace("\"\"URGENTE:", '\"URGENTE:')
        json_str2 = json_str2.replace("2009\".\"", '2009\"')
        json_str2 = json_str2.replace('\\', '\\\\')
        json_str2 = json_str2.replace("True", "true")
        json_str2 = json_str2.replace("False", "false")
        json_str2 = json_str2.replace('None', 'null')
        print(json_str2)
        return json.loads(json_str2)

    def handle_response(response):
        if response.status_code == 200:
            return True
        else:
            print('Erro no response da chamada: satsus_code=', response.status_code)
            return False

    def carrega_dados(game : int, chunck: int):
        try:
            #host_name = 'http://host.docker.internal:8888'
            conn = BaseHook.get_connection('local_reverse_proxy')
            url = conn.host + "/portaldeloterias/api/megasena/" + str(game)
            response = requests.get(url, verify=False)
            result = json.loads(response.content)
            result = fix_json_format(result)
            print(result)
            if handle_response(response) and result is not None and "exception" not in result:
                save_file(result, chunck)
        except Exception as e:
            log.error(e)
            raise AirflowException(e)

    def save_file(val, file_index: int):
        try:
            path = '/opt/airflow/localfiles'
            if os.path.isdir(path):
                os.chdir(path)
            else:
                os.mkdir(path)
                os.chdir(path)

            csv_name = '/resultados.csv'
            if file_index > 0:
                csv_name = '/resultados_chunck' + str(file_index) + '.csv'

            df = pd.DataFrame({'A' : []})
            if os.path.isfile(path + csv_name):
                df = pd.read_csv(path + csv_name)
                print(df['numero'])
            
            result = val
            print('Numero do Resultado', result["numero"])
            if result['numero'] not in pd.DataFrame(df, columns=['numero']).values:
                if df.empty:
                    print('DF is EMPTY')
                    df = pd.json_normalize(result)
                    df2 = df
                else:
                    print('DF is NOT EMPTY')
                    df2 = df.append(pd.json_normalize(result), ignore_index=True, verify_integrity=True)
                print(df2)
                print(df2['numero'])
                #df2.to_parquet(path + '/resultados.parquet')
                df2.to_csv(path + csv_name)
            else:
                print('Resultado já foi carregado')

            return_value = ''
            if not df.empty:
                return_value = df['numero'].to_json(orient='values')

        except Exception as e:
            log.error(e)
            raise AirflowException(e)
        return return_value

    # 1. Check if the API is up
    Verificar_api  = HttpSensor(
        task_id='Verificar_api',
        http_conn_id='local_reverse_proxy',
        endpoint='portaldeloterias/api/megasena/'
    )

    carrega_ultimo_jogo = SimpleHttpOperator(
        task_id='carrega_ultimo_jogo',
        http_conn_id='local_reverse_proxy',
        endpoint='portaldeloterias/api/megasena',
        method='GET',
        response_filter=lambda response: fix_json_format(response.text),
        log_response=True,
        headers={"Content-Type": "application/json"},
        response_check=lambda response: handle_response(response),
        dag=dag
    )
 
    def xcom_SalvaUltimoJogo(ds, **kwargs):
        try:
            val = kwargs['ti'].xcom_pull(key='return_value', task_ids='carrega_ultimo_jogo')

            json_return = save_file(fix_json_format(str(val)), 0)       
        except Exception as e:
            log.error(e)
            raise AirflowException(e)
        return json_return

    SalvaUltimoJogo = PythonOperator(
         task_id = 'SalvaUltimoJogo',
         python_callable=xcom_SalvaUltimoJogo,
         provide_context=True
    )
 
    def xcom_AvaliaJogosNaoSalvos(ds, **kwargs):
        try:
            val = kwargs['ti'].xcom_pull(key='return_value', task_ids='SalvaUltimoJogo')

            print('Retured Value: _', val, '_')
            loaded_games = np.zeros([1], dtype=int)
            print('Init Loaded Games: ',loaded_games)
            if val:
                loaded_games = np.array(json.loads(val), dtype=int)
                print('Loaded: ', loaded_games)
            
            last_game = np.amax(loaded_games)
            if last_game == 0:
                last_game = 2567
            print('Last: ', last_game)
            all_games = np.arange(1, int(last_game), dtype=int)
            #all_games = np.arange(1, 2000, dtype=int)
            print('All: ', all_games)
            print(all_games.size)
            missing_games = np.setdiff1d(all_games, loaded_games)
            print('Missing: ', missing_games)
            print(missing_games.size)
            chunck_games = np.array_split(missing_games, 10)
            print('Chunck: ', chunck_games)
            print(len(chunck_games))

            numpyData = {"array": chunck_games}
            encodedNumpyData = json.dumps(numpyData, cls=NumpyArrayEncoder)
            print(encodedNumpyData)

        except Exception as e:
            log.error(e)
            raise AirflowException(e)
        return encodedNumpyData

    def merge_chunckfiles():
        try:
            path = '/opt/airflow/localfiles/'
            if os.path.isdir(path):
                os.chdir(path)
            else:
                os.mkdir(path)
                os.chdir(path)

            print(path)

            file_list = [path + f for f in os.listdir(path) if f.startswith('resultados')]
            print(file_list)

            csv_list = []
            for file in sorted(file_list):
                csv_list.append(pd.read_csv(file).assign(file_name = os.path.basename(file)))
                print(csv_list)

            csv_merged = pd.concat(csv_list, ignore_index=True)
            print(csv_merged)

            csv_merged.to_csv(path + 'Consolidado.csv', index=False)

            for file in sorted(file_list):
                os.remove(file)

            os.rename('Consolidado.csv', 'resultados.csv')
        except Exception as e:
            log.error(e)
            raise AirflowException(e)

    def format_dataframe():
        try:
            path = '/opt/airflow/localfiles/'
            if os.path.isdir(path):
                os.chdir(path)
            else:
                os.mkdir(path)
                os.chdir(path)

            print(path)

            dataset_list = [path + f for f in os.listdir(path) if f.startswith('dataset')]
            print(dataset_list)
            for data_file in dataset_list:
                os.remove(data_file)

            file_list = [path + f for f in os.listdir(path) if f.startswith('resultados.csv')]
            print(file_list)

            if file_list:
                df = pd.read_csv(file_list[0])
                df = df[['numero','acumulado','dataApuracao','dataProximoConcurso','dezenasSorteadasOrdemSorteio','listaDezenas','listaDezenasSegundoSorteio',
                            'numeroConcursoAnterior','numeroConcursoFinal_0_5','numeroConcursoProximo','numeroJogo','exibirDetalhamentoPorCidade','id','indicadorConcursoEspecial',
                            'listaMunicipioUFGanhadores','listaRateioPremio','listaResultadoEquipeEsportiva','localSorteio','nomeMunicipioUFSorteio','nomeTimeCoracaoMesSorte',
                            'observacao','premiacaoContingencia','tipoJogo','tipoPublicacao','ultimoConcurso','valorArrecadado','valorAcumuladoConcurso_0_5',
                            'valorAcumuladoConcursoEspecial','valorAcumuladoProximoConcurso','valorEstimadoProximoConcurso','valorSaldoReservaGarantidora','valorTotalPremioFaixaUm']]
                df = df.sort_values(by=['numero'], ascending=False)
                df = df.reset_index(drop=True)
                print(df)
                df.to_csv(path + 'dataset_resultados_megasenna.csv', index=True)
                df.to_json(path + 'dataset_resultados_megasenna.json', index=True, orient='index')
                df.to_parquet(path + 'dataset_resultados_megasenna.parquet', index=True)
                df.to_excel(path + 'dataset_resultados_megasenna.xlsx', index=True)

        except Exception as e:
            log.error(e)
            raise AirflowException(e)  

    AvaliaJogosNaoSalvos = PythonOperator(
         task_id = 'AvaliaJogosNaoSalvos',
         python_callable=xcom_AvaliaJogosNaoSalvos,
         provide_context=True
    )

    def xcom_SalvaJogos(ds, chunck: int, **kwargs):
        try:
            val = kwargs['ti'].xcom_pull(key='return_value', task_ids='AvaliaJogosNaoSalvos')

            decodedArrays = json.loads(val)
            finalNumpyArray = np.asarray(decodedArrays["array"])
            print('Chunck: ',chunck)
            print(finalNumpyArray[chunck-1])
            games = finalNumpyArray[chunck-1]
            for game_num in games:
                print('Game:', game_num)
                carrega_dados(game_num, chunck)

        except Exception as e:
            log.error(e)
            raise AirflowException(e)
        
    SalvaJogos1 = PythonOperator(
         task_id = 'SalvaJogos1',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 1}
    )

    SalvaJogos2 = PythonOperator(
         task_id = 'SalvaJogos2',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 2}
    )

    SalvaJogos3 = PythonOperator(
         task_id = 'SalvaJogos3',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 3}
    )

    SalvaJogos4 = PythonOperator(
         task_id = 'SalvaJogos4',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 4}
    )

    SalvaJogos5 = PythonOperator(
         task_id = 'SalvaJogos5',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 5}
    )

    SalvaJogos6 = PythonOperator(
         task_id = 'SalvaJogos6',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 6}
    )

    SalvaJogos7 = PythonOperator(
         task_id = 'SalvaJogos7',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 7}
    )

    SalvaJogos8 = PythonOperator(
         task_id = 'SalvaJogos8',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 8}
    )

    SalvaJogos9 = PythonOperator(
         task_id = 'SalvaJogos9',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 9}
    )

    SalvaJogos10 = PythonOperator(
         task_id = 'SalvaJogos10',
         python_callable=xcom_SalvaJogos,
         provide_context=True,
         op_kwargs={'chunck': 10}
    )

    merge_chunckes = PythonOperator(
         task_id = 'merge_chunckes',
         python_callable=merge_chunckfiles,
         provide_context=True
    )

    reindex_convert_dataframe = PythonOperator(
         task_id = 'reindex_convert_dataframe',
         python_callable=format_dataframe,
         provide_context=True
    )

    Verificar_api >> carrega_ultimo_jogo >> SalvaUltimoJogo >> AvaliaJogosNaoSalvos >> [SalvaJogos1, SalvaJogos2, SalvaJogos3, SalvaJogos4, SalvaJogos5, SalvaJogos6, SalvaJogos7, SalvaJogos8, SalvaJogos9, SalvaJogos10] >> merge_chunckes >> reindex_convert_dataframe