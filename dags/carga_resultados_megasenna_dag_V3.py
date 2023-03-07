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
from airflow.utils.edgemodifier import Label
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='carga_resultados_megasenna_dag_V3', 
    start_date = datetime(2023,1,1),
    #schedule_interval="30 * * * *",
    schedule_interval="@daily", 
    catchup=False
) as dag:

    log = logging.getLogger(__name__)

    global chuncks_num
    global csv_name
    global df

    chuncks_num = 10

    class NumpyArrayEncoder(JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return JSONEncoder.default(self, obj)

    #Prepare DataFrame Resultados    
    path = '/opt/airflow/localfiles/'
    if os.path.isdir(path):
        os.chdir(path)
    else:
        os.mkdir(path)
        os.chdir(path)
    csv_name = '/resultados.csv'
    df = pd.DataFrame({'A' : []})
    if os.path.isfile(path + csv_name):
        df = pd.read_csv(path + csv_name)
        print(df['numero'])

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
        
    def xcom_SalvaUltimoJogo(ds, **kwargs):
        try:
            val = kwargs['ti'].xcom_pull(key='return_value', task_ids='carrega_ultimo_jogo')
            return_value = ''
            print(val)
            global df
            if val:
                result = fix_json_format(str(val))
                if result['numero'] not in pd.DataFrame(df, columns=['numero']).values:
                    df = df.append(pd.json_normalize(result), ignore_index=True, verify_integrity=True)
                    df = df.sort_values(by=['numero'], ascending=False)
                    #df = df.astype({"numero": "int"})
                    df.columns = df.columns.str.strip()
                    df["numero"] = df['numero'].astype('int')
                    df = df.drop_duplicates(subset=['numero'])
                    #df = df.set_index('numero')
                    print(df.dtypes)
                    ti = kwargs['ti']
                    ti.xcom_push('CVS_UPDATED', True)
                    df.to_csv(path + csv_name)
                else:
                    print('Resultado já foi carregado')
            print(df)
            print(df['numero'])
            return_value = df['numero'].to_json(orient='values')
        except Exception as e:
            log.error(e)
            raise AirflowException(e)
        return return_value
        
    def xcom_AvaliaJogosNaoSalvos(ds, **kwargs):
        try:
            ti = kwargs['ti']
            val = ti.xcom_pull(key='return_value', task_ids='SalvaUltimoJogo')

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
            chunck_games = np.array_split(missing_games, int(chuncks_num))
            print('Chunck: ', chunck_games)
            print(len(chunck_games))

            numpyData = {"array": chunck_games}
            encodedNumpyData = json.dumps(numpyData, cls=NumpyArrayEncoder)
            print(encodedNumpyData)

            ti.xcom_push('missing_games', encodedNumpyData)
            
            if missing_games.size > 0:
                action = "connectorOperator"
            elif ti.xcom_pull(key='CVS_UPDATED', task_ids='SalvaUltimoJogo'):
                action = "reindex_convert_dataframe"
            else:
                action = "ending_process"

        except Exception as e:
            log.error(e)
            raise AirflowException(e)
        return action

    def format_dataframe():
            try:
                print(path)

                dataset_list = [path + f for f in os.listdir(path) if f.startswith('dataset')]
                print(dataset_list)
                for data_file in dataset_list:
                    os.remove(data_file)

                df_dataset = df[['numero','acumulado','dataApuracao','dataProximoConcurso','dezenasSorteadasOrdemSorteio','listaDezenas','listaDezenasSegundoSorteio',
                            'numeroConcursoAnterior','numeroConcursoFinal_0_5','numeroConcursoProximo','numeroJogo','exibirDetalhamentoPorCidade','id','indicadorConcursoEspecial',
                            'listaMunicipioUFGanhadores','listaRateioPremio','listaResultadoEquipeEsportiva','localSorteio','nomeMunicipioUFSorteio','nomeTimeCoracaoMesSorte',
                            'observacao','premiacaoContingencia','tipoJogo','tipoPublicacao','ultimoConcurso','valorArrecadado','valorAcumuladoConcurso_0_5',
                            'valorAcumuladoConcursoEspecial','valorAcumuladoProximoConcurso','valorEstimadoProximoConcurso','valorSaldoReservaGarantidora','valorTotalPremioFaixaUm']]
                df_dataset = df_dataset.sort_values(by=['numero'], ascending=False)
                df_dataset["numero"] = df_dataset['numero'].astype('int')
                df_dataset = df_dataset.drop_duplicates(subset=['numero'])
                df_dataset = df_dataset.reset_index(drop=True)
                print(df_dataset)
                df_dataset.to_csv(path + 'dataset_resultados_megasenna.csv', index=True)
                df_dataset.to_json(path + 'dataset_resultados_megasenna.json', index=True, orient='index')
                df_dataset.to_parquet(path + 'dataset_resultados_megasenna.parquet', index=True)
                df_dataset.to_excel(path + 'dataset_resultados_megasenna.xlsx', index=True)

            except Exception as e:
                log.error(e)
                raise AirflowException(e)  

    def merge_chunckfiles():
        try:
            print(path)

            file_list = [path + f for f in os.listdir(path) if f.startswith('resultados')]
            print(file_list)

            csv_list = []
            for file in sorted(file_list):
                csv_list.append(pd.read_csv(file).assign(file_name = os.path.basename(file)))
                print(csv_list)

            csv_merged = pd.concat(csv_list, ignore_index=True)
            csv_merged["numero"] = csv_merged['numero'].astype('int')
            csv_merged = csv_merged.drop_duplicates(subset=['numero'])
            print(csv_merged)

            csv_merged.to_csv(path + 'Consolidado.csv', index=True)

            for file in sorted(file_list):
                os.remove(file)

            os.rename('Consolidado.csv', 'resultados.csv')
        except Exception as e:
            log.error(e)
            raise AirflowException(e)

    def save_file(val, file_index: int, **kwargs):
        try:
            csv_name = '/resultados_chunck' + str(file_index) + '.csv'

            df_chunck = pd.DataFrame({'A' : []})
            if os.path.isfile(path + csv_name):
                df_chunck = pd.read_csv(path + csv_name)
                print(df['numero'])
            
            result = val
            print('Numero do Resultado', result["numero"])
            if result['numero'] not in pd.DataFrame(df_chunck, columns=['numero']).values:
                if df_chunck.empty:
                    print('DF is EMPTY')
                    df_chunck = pd.json_normalize(result)
                else:
                    print('DF is NOT EMPTY')
                    df_chunck = df_chunck.append(pd.json_normalize(result), ignore_index=True, verify_integrity=True)
                print(df_chunck)
                print(df_chunck['numero'])
                df_chunck["numero"] = df_chunck['numero'].astype('int')
                df_chunck = df_chunck.drop_duplicates(subset=['numero'])
                df_chunck.to_csv(path + csv_name)
            else:
                print('Resultado já foi carregado')

        except Exception as e:
            log.error(e)
            raise AirflowException(e)

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
        
    def xcom_SalvaJogos(ds, chunck: int, **kwargs):
        try:
            val = kwargs['ti'].xcom_pull(key='missing_games', task_ids='AvaliaJogosNaoSalvos')

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
                            
    gameday_check = BranchDayOfWeekOperator(
        task_id='gameday_check',
        week_day={WeekDay.WEDNESDAY, WeekDay.THURSDAY, WeekDay.FRIDAY, WeekDay.SATURDAY},
        use_task_logical_date=True,
        follow_task_ids_if_true='Verificar_api',
        follow_task_ids_if_false='ending_process',
        dag=dag)
    
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

    SalvaUltimoJogo = PythonOperator(
         task_id = 'SalvaUltimoJogo',
         python_callable=xcom_SalvaUltimoJogo,
         provide_context=True
    )

    AvaliaJogosNaoSalvos = BranchPythonOperator(
         task_id = 'AvaliaJogosNaoSalvos',
         python_callable=xcom_AvaliaJogosNaoSalvos,
         provide_context=True
    )

    ending_process = EmptyOperator(
        task_id='ending_process'
    )
 
    reindex_convert_dataframe = PythonOperator(
         task_id = 'reindex_convert_dataframe',
         python_callable=format_dataframe,
         provide_context=True
    )

    merge_chunckes = PythonOperator(
         task_id = 'merge_chunckes',
         python_callable=merge_chunckfiles,
         provide_context=True,
         trigger_rule=TriggerRule.ALL_DONE
    )

    connectorOperator = PythonOperator(
        task_id='connectorOperator',
        python_callable=lambda : print('To Passando'),
        provide_context=True
    )

    Label("Verifica se é Dia de Jogo") >> gameday_check >> Label("Verificar se o Serviço está respondendo") >> Verificar_api >> Label("Carrega o Ultimo Jogo") >> carrega_ultimo_jogo >> Label("Salva o Ultimo Jogo") >> SalvaUltimoJogo >> Label("Avalia se existe algum Jogo que ainda não foi carregado") >> AvaliaJogosNaoSalvos
    
    gameday_check >> Label("Serviço Não Responde") >> ending_process
    AvaliaJogosNaoSalvos >> Label("Formata Arquivos de Saída") >> reindex_convert_dataframe
    AvaliaJogosNaoSalvos >> Label("Carga Concluída") >> ending_process

    for chunckid in range(1,int(chuncks_num)+1):
        SalvaJogos = PythonOperator(
            task_id = 'Salva_Jogos' + str(chunckid),
            python_callable=xcom_SalvaJogos,
            provide_context=True,
            op_kwargs={'chunck': int(chunckid)}
        )

        AvaliaJogosNaoSalvos >> Label("Carrega Jogos MultiThreading") >> connectorOperator >> Label("Carrega Jogos") >>  SalvaJogos >> Label("Uni os arquivos de cargas parciais") >> merge_chunckes >> Label("Formata Arquivos de Saída") >> reindex_convert_dataframe >> Label("Carga Concluída") >> ending_process