import yaml
from dw import dict_modelo
import gcsfs
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import io
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

    fs = gcsfs.GCSFileSystem()

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

    for tabela in modelo['dimensoes']:
        #print(modelo['tabelas'][tabela]['bigquery_name'])

        gcs_path = modelo['dimensoes'][tabela]['gcs_path']
        extensao = modelo['dimensoes'][tabela]['extension']
        encoding = modelo['dimensoes'][tabela]['encoding']
        sep = modelo['dimensoes'][tabela]['sep']
        surrogate = modelo['dimensoes'][tabela]['surrogate_key'][0]
        natural = modelo['dimensoes'][tabela]['natural_keys'][0]
        fields_for_updates = modelo['dimensoes'][tabela]['fields_for_updates']

        # método para ler todos os arquivos da pasta. este método nao permite identificar quais arquivos serão lidos
        # df = dd.read_csv(f'gcs://{bucket_dw}/silver/{gcs_path}/{tabela}.{extensao}', encoding=encoding, sep=sep)

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
            df = pd.concat(lst_dfs, ignore_index=True)
        except:
            df = pd.DataFrame()

        if not df.empty:
            bigquery_name = modelo['dimensoes'][tabela]['bigquery_name']
            dataset, table = bigquery_name.split(".")

            table_ref = client_bq.dataset(dataset).table(table)

            # ---- Verifique se a tabela existe ----
            adicionado_a_lista = False
            try:
                checkTable = client_bq.get_table(table_ref, retry=None)
                print(f"A tabela {table} já existe no BigQuery. Verificações serão feitas...")

                fazVerificacoes = True

            except:
                #quando a tabela nao existe no bq, entao somente add o contador para todo o dataframe e depois carrega pro bigquery
                df[surrogate] = df.reset_index().index + 1

                try:
                    df.to_gbq(modelo['dimensoes'][tabela]['bigquery_name'], project_id)
                    print(f"A tabela {table} não existia no BigQuery mas foi criada.")

                    if not adicionado_a_lista:
                        lst_dfs_executados.append(pd.DataFrame({'arquivo': id_file}, index=[0])) #index[0] pois tem somente 1 linha no dict
                        adicionado_a_lista = True

                    fazVerificacoes = False

                except Exception as e: 
                    print(f"A tabela {table} não existe no BigQuery e não foi possível criar devido ao erro {e}")

                    fazVerificacoes = False

            # ---- cria o dataframe para verificacoes // cenario onde a tabela ja existe ----
            if fazVerificacoes: 
                df_table = pd.read_gbq(f"select {natural}, {surrogate} from {bigquery_name} order by {surrogate} desc", project_id=project_id)
                #print(df_table)
                maxid = df_table.iloc[0][surrogate]

                print(f"...P: Há novas linhas para serem inseridas? ")
                df_novas_linhas = df.merge(df_table, on=natural
                                            ,how='outer', indicator=True, suffixes=('', '_y')).query('_merge == "left_only"').drop(columns='_merge')
                
                if df_novas_linhas.shape[0] == 0:
                    print(f"...R: Não há novas linhas.")

                else:
                    df_novas_linhas = df_novas_linhas.drop(columns=df_novas_linhas.filter(regex='_y$').columns)
                    df_novas_linhas.reset_index()
                    df_novas_linhas[surrogate] = df_novas_linhas.reset_index().index + maxid + 1

                    df_novas_linhas.to_gbq(modelo['dimensoes'][tabela]['bigquery_name'], project_id, if_exists="append")
                    print("...R: Novas linhas foram inseridas.") 

                    if not adicionado_a_lista:
                        lst_dfs_executados.append(pd.DataFrame({'arquivo': id_file}, index=[0])) #index[0] pois tem somente 1 linha no dict
                        adicionado_a_lista = True


                print(f"...P: Há linhas para serem atualizadas? ")

                if not adicionado_a_lista:
                    lst_dfs_executados.append(pd.DataFrame({'arquivo': id_file}, index=[0])) #index[0] pois tem somente 1 linha no dict
                    adicionado_a_lista = True

                df_para_atualizar = df.merge(df_table, on=natural
                                            ,how='inner')
                
                df_para_atualizar = df_para_atualizar.drop(columns=df_para_atualizar.filter(regex='_y$').columns)
                df_para_atualizar.rename(columns={})

                #print(df_para_atualizar)

                if df_para_atualizar.shape[0] > 0:
                    stg_table = modelo['dimensoes'][tabela]['bigquery_name'].replace(".dim", ".stg").replace(dataset, "staging")
                    df_para_atualizar.to_gbq(stg_table, project_id, if_exists="replace")

                    atualizaveis = ""
                    verificaveis = ""
                    for i in fields_for_updates:
                        atualizaveis = atualizaveis + "data." + str(i) + " = staging." + str(i) + ","
                        verificaveis = verificaveis + "data." + str(i) + " <> staging." + str(i) + " or "

                    atualizaveis = atualizaveis[:-1]
                    verificaveis = verificaveis[:-3]

                    strSQL = f"""
                        MERGE {bigquery_name} data
                        USING {stg_table} staging
                        ON
                        staging.{natural} = data.{natural}
                        WHEN MATCHED AND ({verificaveis}) THEN
                        UPDATE SET
                            {atualizaveis}
                    """
                    
                    job = client_bq.query(strSQL) 
                    job.result() #aguarda o job finalizar

                    print(f"...R: As chaves iguais foram encaminhadas ao BQ. Se houve qualquer alteração, foi realizado direto pelo BigQuery.")

                else:
                    print(f"...R: Não há linhas para serem atualizadas.")

    # ---- empilha os dataframes dos nomes de arquivos ----
    if len(lst_dfs_executados) > 0:
        df_arquivos_lidos = pd.concat(lst_dfs_executados, ignore_index=True)

    else:
        df_arquivos_lidos = pd.DataFrame()

    if not df_arquivos_lidos.empty:
        return {"df_arquivos_runtime": df_arquivos_lidos
                ,"deve-rodar": True}
    
    else:
        return {"df_arquivos_runtime": df_arquivos_lidos
                ,"deve-rodar": False}
        
if __name__ == '__main__':
    print(rodar())



