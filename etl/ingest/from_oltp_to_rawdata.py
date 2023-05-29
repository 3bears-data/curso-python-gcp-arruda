import psycopg2
import pandas as pd
import yaml
from google.cloud import storage

if __name__ == '__main__':
    with open(r'C:\Users\leand\Desktop\py-gcp\config\config.yaml') as file:
        config = yaml.safe_load(file)

    db_name = config['connector-oltp']['db_name']
    db_user = config['connector-oltp']['db_user']
    db_password = config['connector-oltp']['db_password']
    db_host = config['connector-oltp']['db_host']
    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']
    db_iduser = config['connector-oltp']['db_iduser']

    # ---- conecte ao banco de dados ----
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host)

    # ---- obtem a lista de tabela ----
    query = f"""
        SELECT n.nspname AS schema,
            t.relname AS table_name,
            t.relkind AS type,
            t.relowner::regrole AS owner,
            t.relowner 
        FROM pg_class AS t
        JOIN pg_namespace AS n ON t.relnamespace = n.oid
        /* only tables and partitioned tables */
        WHERE t.relkind IN ('r', 'p') and t.relowner = {db_iduser} 
        """  
    
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()

    tabelas = [resultado[1] for resultado in results]

    tabelas_permitidas = ['customers', 'employees', 'categories', 'products', 'suppliers', 'orders', 'shippers', 'region', 'territories', 'order_details']

    tabelas_interseccao = list(set(tabelas) & set(tabelas_permitidas))

    #setar a variavel do storage
    client_storage = storage.Client.from_service_account_json(credentials_path)

    for tabela in tabelas_interseccao:
        df = pd.read_sql_query(f"select * from {tabela}", conn)
        
        if not df.empty:
            folder_path = f"oltp-north/{tabela}/{tabela}.csv"
            bucket = client_storage.bucket(bucket_raw)

            #cria csv a partir do df
            csv_data = df.to_csv(index=False, encoding='utf-8-sig', sep=";")
            csv_data = csv_data.encode('iso-8859-1')

            #upload pro storage
            blob = bucket.blob(folder_path)
            blob.upload_from_string(csv_data)

            #exemplo gs://
            #if tabela == "order_details":
            #    df.to_csv(f"gs://{bucket_raw}/{folder_path}", encoding="utf-8-sig", sep=";", index=False)

            print(f"Tabela {tabela} carregada no destino")

        else:
            print(f"Tabela {tabela} vazia, por tanto, não carregado ao storage")






        
    
    

      

    

    

    


