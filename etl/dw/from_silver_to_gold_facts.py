import yaml
from dw import dict_modelo
import gcsfs
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import io
import datetime
import dask.dataframe as dd 


#if __name__=='__main__':
def rodar():
    # ---- alimentar as variaveis a partir do yaml ----
    with open(r'C:\Users\leand\Desktop\py-gcp\config\config.yaml') as file:
        config = yaml.safe_load(file)

    bucket_dw  = config['bucket-negocio']
    credentials_path = config['credentials_path']
    bucket_processed = config['bucket-processado']
    project_id = config['project-id']

    client_bq = bigquery.Client.from_service_account_json(credentials_path)
    modelo = dict_modelo.dict_for_model

    # cria lista de arquivos ja rodados antes, ou seja, legado
    try:
        df_arquivos_legado = dd.read_csv(f'gs://{bucket_processed}/from-silver-to-gold/*', encoding='iso-8859-1', sep=";")
        df_arquivos_legado = df_arquivos_legado.compute() # Converte o DataFrame do dask para pandas

    except:
        df_arquivos_legado = pd.DataFrame({'arquivo': None}, index=[0])

    try:
        lst_legado = df_arquivos_legado['arquivo'].to_list()
    except:
        lst_legado = []

    lst_dfs_executados = []

    for tabela in modelo['fatos']:
        #print(modelo['tabelas'][tabela]['bigquery_name'])

        gcs_path = modelo['fatos'][tabela]['gcs_path']
        extensao = modelo['fatos'][tabela]['extension']
        encoding = modelo['fatos'][tabela]['encoding']
        sep = modelo['fatos'][tabela]['sep']
        surrogate = modelo['fatos'][tabela]['surrogate_key'][0]
        natural = modelo['fatos'][tabela]['natural_keys'][0]
        fields_for_updates = modelo['fatos'][tabela]['fields_for_updates']
        dims = modelo['fatos'][tabela]['dims']
        persist = modelo['fatos'][tabela]['persist']
        incremental_by = modelo['fatos'][tabela]['incremental_by']

        lst_dims = [dim for dim in dims]
        lst_fields_dims = [modelo['fatos'][tabela]['dims'][dim] for dim in dims]
        lst_all_fields = lst_fields_dims + fields_for_updates + persist


        # método para ler todos os arquivos da pasta. este método nao permite identificar quais arquivos serão lidos
        #df = dd.read_csv(f'gcs://{bucket_dw}/silver/{gcs_path}/{tabela}.{extensao}', encoding=encoding', sep=sep)
        #df = df.compute() # Converte o DataFrame do dask para pandas

        # faz um loop pelos arquivos encontrados em silver
        client_storage = storage.Client.from_service_account_json(credentials_path)
        bucket = client_storage.get_bucket(bucket_dw)
        
        blob_list = list(bucket.list_blobs(prefix=f'silver/{gcs_path}/{tabela}'))

        lst_dfs = []

        #inicia validacao se deve ou nao rodar em todos os arquivos do loop
        for blob in blob_list:
            id_file = f"{blob.name}_{blob.updated.strftime('%Y%m%d%H%M%S')}"

            if not id_file in lst_legado: #verificacao se deve ou nao rodar
                print(f"\nArquivo {blob.name} será processado para gold pois é novo.")
                deve_rodar = True
            else:
                print(f"\nArquivo {blob.name} não será processado para gold pois já foi alimentado no dw antes.")
                deve_rodar = False

            if deve_rodar:
                file_bytes = blob.download_as_bytes()
                file_buffer = io.BytesIO(file_bytes)
                df = pd.read_csv(file_buffer, encoding='iso-8859-1', sep=";")

                lst_dfs.append(df)

        try:
            df_fato = pd.concat(lst_dfs, ignore_index=True)
        except:
            df_fato = pd.DataFrame()

        if not df_fato.empty:
            df_fato = df_fato[lst_all_fields].copy()

            # amarra o csv do storage com as dimensoes para capturar os id's
            for dimensao, campo in zip(lst_dims, lst_fields_dims):
                surrogate = modelo['dimensoes'][dimensao]['surrogate_key'][0]
                bigquery_name = modelo['dimensoes'][dimensao]['bigquery_name']
                natural = modelo['dimensoes'][dimensao]['natural_keys'][0]

                df_dimensao = pd.read_gbq(f"select {natural}, {surrogate} from {bigquery_name} order by {surrogate} desc", project_id=project_id)
                df_fato = pd.merge(df_fato, df_dimensao
                                ,how="inner"
                                ,on=campo)
                
            #remove as colunas de natural key
            df_fato.drop(columns=lst_fields_dims, inplace=True)

            bigquery_name = modelo['fatos'][tabela]['bigquery_name']

            # ---- Verifique se a tabela existe ----
            dataset, table = bigquery_name.split(".")
            table_ref = client_bq.dataset(dataset).table(table)

            try:
                checkTable = client_bq.get_table(table_ref, retry=None)
                print(f"A tabela {table} já existe no BigQuery. Incremental será executado.")

                fazIncremental = True

            except:
                print(f"A tabela {table} não existe no BigQuery. Uma carga full será executada.")

                fazIncremental = False
        

            if fazIncremental: 
                #incremental pelo campo...
                lst_incremental = df_fato[incremental_by].unique().tolist()

                # Cria a query para excluir as linhas da tabela com as datas especificadas
                strSQL = f"""
                    DELETE FROM {bigquery_name}
                    WHERE {incremental_by} IN ({','.join([f'"{date}"' for date in lst_incremental])})
                """

                job = client_bq.query(strSQL) 
                job.result() #aguarda o job finalizar

            # executa a carga
            dtCarga = datetime.datetime.now()
            df_fato['dtCarga'] = dtCarga
            
            df_fato.to_gbq(modelo['fatos'][tabela]['bigquery_name'], project_id)

            lst_dfs_executados.append(pd.DataFrame({'arquivo': id_file}, index=[0])) #index[0] pois tem somente 1 linha no dict


    # ---- empilha os dataframes dos nomes de arquivos ----
    if len(lst_dfs_executados) > 0:
        df_arquivos_lidos = pd.concat(lst_dfs_executados, ignore_index=True)

    else:
        df_arquivos_lidos = pd.DataFrame()

    if not df_arquivos_lidos.empty:
        # se arquivo fato foi processado entao pode recriar a dim_calendario
        strSQL = """
        CREATE OR REPLACE VIEW `dw_north_sales.dim_calendario` AS
        SELECT
        calendar_date,
        EXTRACT(YEAR FROM calendar_date) AS year,
        EXTRACT(MONTH FROM calendar_date) AS month,
        EXTRACT(DAY FROM calendar_date) AS day,
        EXTRACT(DAYOFWEEK FROM calendar_date) AS day_of_week,
        EXTRACT(QUARTER FROM calendar_date) AS quarter,
        EXTRACT(WEEK FROM calendar_date) AS week,
        EXTRACT(DAYOFYEAR FROM calendar_date) AS day_of_year,
        DATE_TRUNC(calendar_date, MONTH) AS first_day_of_month,
        DATE_TRUNC(calendar_date, QUARTER) AS first_day_of_quarter,
        DATE_TRUNC(calendar_date, YEAR) AS first_day_of_year,
        DATE_ADD(DATE_TRUNC(calendar_date, WEEK(MONDAY)), INTERVAL 6 DAY) AS week_ending_on
        FROM
        UNNEST(GENERATE_DATE_ARRAY(
            DATE((SELECT MIN(order_date) FROM dw_north_sales.fato_pedidos)),
            DATE((SELECT MAX(order_date) FROM dw_north_sales.fato_pedidos))
        )) AS calendar_date;
        
        """

        job = client_bq.query(strSQL) 
        job.result() #aguarda o job finalizar

        return {"df_arquivos_runtime": df_arquivos_lidos
                ,"deve-rodar": True}
    
    else:
        return {"df_arquivos_runtime": df_arquivos_lidos
                ,"deve-rodar": False}


if __name__ == '__main__':
    print(rodar())



