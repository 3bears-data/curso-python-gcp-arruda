import pandas as pd
import io
from google.cloud import storage
import yaml
import dask.dataframe as dd


def rodar():
    # ---- alimentar as variaveis a partir do yaml -----
    with open(r'C:\Users\leand\Desktop\py-gcp\config\config.yaml') as file:
        config = yaml.safe_load(file)

    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']
    bucket_processed = config['bucket-processado']   

    # captura dos dataframes
    parent_folder = "oltp-north"
    objetos = {"df_orders": {"pasta": 'orders', "df": None}
               ,"df_order_details": {"pasta": 'order_details', "df": None}
               ,"df_products": {"pasta": 'products', "df": None}
               ,"df_employees": {"pasta": 'employees', "df": None}
               ,"df_customers": {"pasta": 'customers', "df": None}
               ,"df_shippers": {"pasta": 'shippers', "df": None}
               ,"df_categories": {"pasta": 'categories', "df": None}
               ,"df_suppliers": {"pasta": 'suppliers', "df": None}
               }
    
    lst_dfs_executados = []

    # cria lista de arquivos ja rodaram, ou seja, o legado/processados
    try:
        df_arquivos_legado = dd.read_csv(f"gs://{bucket_processed}/from-rawdata-to-silver/*", encoding='iso-8859-1', sep=";")
        df_arquivos_legado = df_arquivos_legado.compute()

    except:
        df_arquivos_legado = pd.DataFrame({'arquivo': None}, index=[0])

    lst_legado = df_arquivos_legado['arquivo'].to_list()
    
    # inicia o loop para cada item do nosso dicionario
    #print(objetos['df_products']['pasta'])
    
    for i in objetos:
        pasta = objetos[i]['pasta']
        client_storage = storage.Client.from_service_account_json(credentials_path)

        bucket = client_storage.get_bucket(bucket_raw)
        blob_list = list(bucket.list_blobs(prefix=f"{parent_folder}/{pasta}"))

        lst_dfs=[]
        
        for blob in blob_list:
            id_file = f"{blob.name}_{blob.updated.strftime('%Y%m%d%H%M%S')}"

            if 'orders' in blob.name or 'order_details' in blob.name:
                if not id_file in lst_legado:
                    print(f"Arquivo {blob.name} será processado para silver pois é novo.")
                    lst_dfs_executados.append(pd.DataFrame({'arquivo': id_file}, index=[0]))

                else:
                    print(f"Arquivo {blob.name} não sera processado para silve rpois já foi alimentado no dw antes")

                    return {"deve-rodar": False}
                
            file_bytes = blob.download_as_bytes()
            file_buffer = io.BytesIO(file_bytes)
            df = pd.read_csv(file_buffer, encoding="iso-8859-1", sep=";")

            lst_dfs.append(df)
        
        # empilha os dataframes lidos do rawdata
        combined_df = pd.concat(lst_dfs, ignore_index=True)
        objetos[i]['df'] = combined_df

    # ---- empilha os dataframes dos nomes de arquivos ----
    if len(lst_dfs_executados) > 0:
        df_arquivos_lidos = pd.concat(lst_dfs_executados, ignore_index=True)

    return {'deve-rodar': True
            ,'objetos': objetos
            ,'df_arquivos_runtime': df_arquivos_lidos
    }
    
if __name__ == '__main__':
    rodar()
