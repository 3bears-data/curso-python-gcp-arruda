from google.cloud import storage
import yaml

def rodar(dict_objetos_finais):
    # ---- alimentar as variaveis a partir do yaml ----
    with open(r'C:\Users\leand\Desktop\py-gcp\config\config.yaml') as file:
        config = yaml.safe_load(file)

    bucket_dw  = config['bucket-negocio']
    credentials_path = config['credentials_path']

    # cria o client do storage
    client_storage = storage.Client.from_service_account_json(credentials_path)
    bucket = client_storage.bucket(bucket_dw)  
    
    for tabela in dict_objetos_finais:
        try:
            df = dict_objetos_finais[tabela]['df_final']

            # crie um arquivo CSV com o dataframe
            csv_data = df.to_csv(index=False, encoding="utf-8-sig", sep=";")
            csv_data = csv_data.encode('iso-8859-1')

            # carregue o arquivo para o Cloud Storage
            bucket_path = f"silver/{tabela}/{tabela}.csv"
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_data, content_type="csv")

        except Exception as e:
            print("Teve erro aqui")
            return {"deve-rodar": False
                    ,"description": e}
        
    return {"deve-rodar": True
            ,"description": None}