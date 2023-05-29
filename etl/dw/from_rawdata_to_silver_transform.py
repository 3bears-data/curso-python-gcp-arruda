import pandas as pd

def rodar(objetos):
    # ---- fazendo joins/merges ----
    df_merge_orders = pd.merge(objetos['df_orders']['df'], objetos['df_order_details']['df']
                            ,how="left"
                            ,on="order_id")
    
    df_orders_w_shippers = pd.merge(df_merge_orders, objetos['df_shippers']['df']
                    ,how="left"
                    ,left_on="ship_via", right_on="shipper_id")


    df_orders_w_customers = pd.merge(df_orders_w_shippers, objetos['df_customers']['df']
                                    ,how="left"
                                    ,on="customer_id")

    df_orders_w_employees = pd.merge(df_orders_w_customers, objetos['df_employees']['df']
                                    ,how="left"
                                    ,on="employee_id")
    
    
    df_products_w_categories = pd.merge(objetos['df_products']['df'], objetos['df_categories']['df']
                                    ,how="left"
                                    ,on="category_id")
    
    df_orders_w_products = pd.merge(df_orders_w_employees, df_products_w_categories
                                    ,how="left"
                                    ,on="product_id")
       
    df_orders_w_suppliers = pd.merge(df_orders_w_products, objetos['df_suppliers']['df']
                                    ,how="left"
                                    ,on="supplier_id")

    #df_orders_w_suppliers.to_excel(r"C:\Users\leand\Desktop\py-gcp\etl\resultado.xlsx", index=False)
    tabelao = df_orders_w_suppliers.copy()

    # ---- lê a matriz para filtrar as colunas ----
    df_matriz_colunas = pd.read_excel(r"C:\Users\leand\Desktop\py-gcp\docs\matriz_colunas_do_modelo.xlsx", sheet_name="Planilha1", engine="openpyxl")
    #se fosse .xls, a engine seria xlrd

    objetos_do_modelo = ['fato_pedidos','dim_produtos','dim_categorias','dim_entregadores','dim_clientes','dim_funcionarios','dim_fornecedores']

    # criar um dicionário para armazenar todos os objetos e seus DataFrames finais
    dict_objetos_finais = {}

    for item in objetos_do_modelo:
        # filtra a matriz
        df_filtrado = df_matriz_colunas[['colunas','renomear', item]].query(f"{item}=='x'").copy()

        # selecionar as colunas do DataFrame final usando o DataFrame filtrado correspondente
        cols = df_filtrado['colunas'].tolist()
        df_final = tabelao[cols].copy()

        # renomeia as colunas se for necessario
        df_rename = df_filtrado[df_filtrado['renomear'].notnull()]

        if df_rename.shape[0] > 0: #verifica se tem linhas no dataframe
            df_rename = df_rename[['colunas', 'renomear']].copy()

            dict_rename = df_rename.to_dict(orient='list') #necessario fazer o zip
            dict_rename = {chave: valor for chave, valor in zip(dict_rename['colunas'], dict_rename['renomear'])}

            #print(dict_rename)

            df_final.rename(columns=dict_rename, inplace=True)

        # se for dimensão, remove a duplicidade das linhas
        if "dim" in item: 
            df_final = df_final.drop_duplicates()

        # aplica o obs se for a dim_funcionarios
        if "dim_funcionarios" in item: 
            df_final['full_name'] = df_final['first_name'].str.cat(df_final['last_name'], sep=' ')


        # adicionar o objeto e o DataFrame final ao dicionário
        dict_objetos_finais[item] = {"name": item
                                     ,"df_final": df_final
                                     ,"matriz_filtrada": df_filtrado}
        

    # retorno 
    if len(dict_objetos_finais) > 0:
        return {"deve-rodar": True
                ,"dict_objetos_finais": dict_objetos_finais} 
    
    else:
        return {"deve-rodar": False}
    




