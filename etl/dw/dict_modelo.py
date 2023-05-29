dict_for_model = {
    "dimensoes": {
        "dim_produtos": 
        {  "name": "dim_produtos"
            ,"gcs_path": "dim_produtos"
            ,"bigquery_name": "dw_north_sales.dim_produtos"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['product_id']
            ,"fields_for_updates": ['product_name', 'discontinued']
            ,"surrogate_key": ['id_dim_produto']
        }

        ,"dim_categorias": 
        {  "name": "dim_categorias"
            ,"gcs_path": "dim_categorias"
            ,"bigquery_name": "dw_north_sales.dim_categorias"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['category_id']
            ,"fields_for_updates": ['category_name']
            ,"surrogate_key": ['id_dim_categorias']
        }

        ,"dim_entregadores": 
        {  "name": "dim_entregadores"
            ,"gcs_path": "dim_entregadores"
            ,"bigquery_name": "dw_north_sales.dim_entregadores"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['shipper_id']
            ,"fields_for_updates": ['company_name_shipper', 'phone_shipper']
            ,"surrogate_key": ['id_dim_entregadores']
        }

        ,"dim_clientes": 
        {  "name": "dim_clientes"
            ,"gcs_path": "dim_clientes"
            ,"bigquery_name": "dw_north_sales.dim_clientes"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['customer_id']
            ,"fields_for_updates": ['company_name', 'contact_name', 'phone', 'fax']
            ,"surrogate_key": ['id_dim_clientes']
        }

        ,"dim_funcionarios": 
        {  "name": "dim_funcionarios"
            ,"gcs_path": "dim_funcionarios"
            ,"bigquery_name": "dw_north_sales.dim_funcionarios"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['employee_id']
            ,"fields_for_updates": ['city', 'region', 'postal_code', 'country']
            ,"surrogate_key": ['id_dim_funcionarios']
        }

        ,"dim_fornecedores": 
        {  "name": "dim_fornecedores"
            ,"gcs_path": "dim_fornecedores"
            ,"bigquery_name": "dw_north_sales.dim_fornecedores"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['supplier_id']
            ,"fields_for_updates": ['city', 'region', 'postal_code', 'country', 'phone', 'fax', 'contact_name', 'contact_title']
            ,"surrogate_key": ['id_dim_fornecedores']
        }
    }
    ,"fatos": {        
        "fato_pedidos": 
        {  "name": "fato_pedidos"
            ,"gcs_path": "fato_pedidos"
            ,"bigquery_name": "dw_north_sales.fato_pedidos"
            ,"extension": "csv"
            ,"sep": ";"
            ,"encoding": "iso-8859-1"
            ,"natural_keys": ['order_id','id_dim_clientes', 'id_dim_funcionarios','id_dim_produto','id_dim_entregadores','id_dim_fornecedores','id_dim_categorias']
            ,"persist": ['order_id', 'order_date']
            ,"incremental_by": "order_date"
            ,"fields_for_updates": ['shipped_date', 'freight', 'unit_price', 'quantity', 'discount', 'units_in_stock', 'units_on_order']
            ,"surrogate_key": ['order_id','id_dim_clientes', 'id_dim_funcionarios','id_dim_produto','id_dim_entregadores','id_dim_fornecedores','id_dim_categorias']
            ,"dims": {"dim_clientes": "customer_id"
                      ,"dim_produtos":"product_id"
                      ,"dim_fornecedores":"supplier_id"
                      ,"dim_funcionarios":"employee_id"
                      ,"dim_entregadores":"shipper_id" 
                      ,"dim_categorias":"category_id"

            }
        }
    }
}

