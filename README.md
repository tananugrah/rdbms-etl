# rdbms-etl
extract data from data source and load to bigquery

Use Case :
1. Make database connection to Postgresql ,Mysql and Google Bigquery using python
2. Extract data from Postgresql and Mysql databases
3. Create dataset and table schema on bigquery
4. Insert data into BigQuery table
5. Insert data into Bigquery table with the upsert methodology

Program Flow
![alur program](https://user-images.githubusercontent.com/22236787/204204419-4df598d7-b05b-43d2-8eed-bca0813dd0e7.png)

How to running program

1. Make database connection to postgresql, Mysql and Google Big Query

  #in Main.py
  
  
  variable_name = loop.run_until_complete(
         Connection.DBClient.Name_Function()
     )
  
  
    #### set connection postgre ####
    conn_pg = loop.run_until_complete(
        Connection.DBClient.ConnectionDB()
    )
    
    ### set connection mysql ####
    conn_mysql = loop.run_until_complete(
        Connection.DBClient.ConnectionMysql()
    )
    ### set connection BigQuery ####
    conn_BQ = loop.run_until_complete(
        Connection.DBClient.ConnectionBQ()
    )
   #Run in terminal
   
   $python3 app/Main.py
   
2. Extract data from Postgresql and Mysql databases
   in Main.py
   
    ##### extract data postgre db table actor #######
    variable_name = loop.run_until_complete(
        DataOperationBase.name_fuction(
            connection = name_connection,
            query = QueryServices.name_statement.format(
                Column = 'column1,column2,column3,column4',
                Table = 'name_table',
                FilterColumn = ""
            )   
        )
    )
    df = pd.DataFrame(data = name_table, columns = ['column1', 'column2',\
                                                'column3','column4'])
    print(df)
    
   example:
   
    ##### extract data postgre db table actor #######
    actor = loop.run_until_complete(
        DataOperationBase.Extract(
            connection = conn_pg,
            query = QueryServices.select.format(
                Column = 'actor_id, first_name, last_name, last_update',
                Table = 'actor',
                FilterColumn = ""
            )   
        )
    )
    df = pd.DataFrame(data = actor, columns = ['actor_id', 'first_name','last_name','last_update'])
    print(df)
 
 
3. Create dataset and table schema on bigquery

    ### bigquery create dataset ###
    #
    BQ_create_dataset_1 = loop.run_until_complete(DataOperationBase.create_dataset(
                            connection = conn_BQ,dataset_id="digital-method-307808.data_source_pg"))
                        
                        
    ### big query create table actor from source postgre db ####
    ###create table if exist
    #
    BQ_create_table = loop.run_until_complete(
                        DataOperationBase.bq_create_table(
                        connection = conn_BQ,
                        dataset = 'data_source_pg',
                        table = 'PROD_actor',
                        set_schema = [
                                    bigquery.SchemaField('actor_id', 'INTEGER', mode='REQUIRED'),
                                    bigquery.SchemaField('first_name', 'STRING', mode='REQUIRED'),
                                    bigquery.SchemaField('last_name', 'STRING', mode='REQUIRED'),
                                    bigquery.SchemaField('last_update', 'TIMESTAMP', mode='REQUIRED')
                                    ]
        )
    )
    

    ### big query create table as select actor from source postgre db ###
    #
    BQ_create_table_ctas = loop.run_until_complete(
                            DataOperationBase.bq_create_table_ctas(
                            connection = conn_BQ,
                            dataset = 'data_source_pg',
                            new_table = 'STAG_actor',
                            query = QueryServices.create_table_ctas.format(
                                New_Table = 'digital-method-307808.data_source_pg.STAG_actor',
                                Column = '*',
                                Table = 'digital-method-307808.data_source_pg.PROD_actor'
                            )
                        )
                    )
                    
                    
4. Insert data into BigQuery table

   ### big query insert data to table STAG ####
   ###https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
   #
    BQ_Insert_STAG_actor = loop.run_until_complete(
        DataOperationBase.insert_to_BQ(
            connection = conn_pg,
            query = QueryServices.select.format(
                Column = 'actor_id, first_name, last_name,last_update',
                Table = 'actor',
                FilterColumn = ''
            ),
            column = ('actor_id', 'first_name', 'last_name','last_update'),
            BQ_connection = conn_BQ,
            project_id = "digital-method-307808",
            table_id = "data_source_pg.STAG_actor")
    )
  
5. Insert data into Bigquery table with the upsert methodology
    create query store procedure upsert in sql console bigquery
    
    in python script:
        #  ############### upsert employees #####################
    run_procedure_upsert_employees = loop.run_until_complete(
        DataOperationBase.run_procedure(
            connection = conn_BQ,
            query = QueryServices.run_procedure.format(
                project_dataset = 'digital-method-307808.data_source_mysql',
                procedure = 'upsert',
                table_data = 'PROD_employees',
                table_change = 'STAG_employees',
                primary_key = 'employeeNumber'
            ),
            table_id = "data_source_mysql.PROD_employees"
        )
    )
