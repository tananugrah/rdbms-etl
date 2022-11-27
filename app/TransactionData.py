import os
import sys

sys.path.append(os.getcwd())

import asyncio
import pandas as pd
import pandas_gbq
import pyarrow as pa

import asyncpg
import mysql.connector

from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest


class DataOperationBase():

    ##### Running Extract Data Postgres########
    async def Extract(connection,query):
        try:
            record = await connection.fetch(query)
        except asyncpg.PostgresError as exc:
            return ("Failed to extract data ",exc)
        else:
            return record
            await connection.close()
    
    ##### Running Extract Data Mysql########
    async def Extract_Mysql(connection,query):
        try:
            cnx = connection
            cursor = cnx.cursor(buffered=True)
            mySql_select_Query = query
            cursor.execute(mySql_select_Query)
            record = cursor.fetchall()
            return record
        except mysql.connector.Error as error:
            print("Error while connecting to MySQL", error)
        else:
            cursor.close()
            cnx.close()
    
    ##### Create data set in Big Query ########
    async def create_dataset(connection,dataset_id):
        try: 
            # Construct a BigQuery client object.
            client = bigquery.Client()
            #Set dataset_id to the ID of the dataset to create.
            dataset_ref = bigquery.DatasetReference.from_string\
                            (dataset_id, default_project=client.project)
            # Construct a full Dataset object to send to the API
            dataset = bigquery.Dataset(dataset_ref)
            # Specify the geographic location where the dataset should reside.
            dataset.location = "US"
            # Send the dataset to the API for creation, with an explicit timeout.
            # Raises google.api_core.exceptions.Conflict if the Dataset already
            # exists within the project.
            dataset = client.create_dataset(dataset)
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        except:
            print("error")
        
    ###### create table in big query ##########
        #Create a table if not existing
    async def bq_create_table(connection,dataset,table,set_schema):
        client = bigquery.Client() 
        dataset_ref = client.dataset(dataset)
         # Prepares a reference to the table
         #table name
        table_ref = dataset_ref.table(table)
        try:
            client.get_table(table_ref)
            print("Table {} already exists.".format(table_ref))
        except NotFound:
            schema = set_schema
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print('table {} created.'.format(table.table_id))

    ###### big query create table as select ########            
    # #Create a table from a query result ctas
    async def bq_create_table_ctas(connection,query,dataset,new_table,):
        client = bigquery.Client()
        dataset_ref = client.dataset(dataset)
        table_ref = dataset_ref.table(new_table)
        try:
            client.get_table(table_ref)
            print("Table {} already exists.".format(table_ref))
        except NotFound:
            query_job = client.query(query)
            result = query_job.result()
            print("success create table", result)

 
    #### postgre INSERT db into big query ########
    async def insert_to_BQ(connection,query,column,BQ_connection,project_id,table_id):
        record = await connection.fetch(query)
        df = pd.DataFrame(record, columns = column)
        # print(df)
        BQ_conn = BQ_connection
        client = bigquery.Client()
        # # TODO: Set project_id to your Google Cloud Platform project ID.
        # # project_id = "my-project"
        # # TODO: Set table_id to the full destination table ID (including the dataset ID).
        # # table_id = 'my_dataset.my_table'
        pandas_gbq.to_gbq(df, table_id, project_id=project_id,if_exists='replace')
        table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows and {} column to {}".format(table.num_rows,len(table.schema), table_id))

    #### mysql INSERT db into big query ######## using pandas_gbq
    async def mysql_insert_to_BQ_gbq(connection,query,column,BQ_connection,table_id,project_id):
        cnx = connection
        cursor = cnx.cursor(buffered=True)
        mySql_select_Query = query
        cursor.execute(mySql_select_Query)
        record = cursor.fetchall()
        df = pd.DataFrame(record, columns = column)
        # print(df)
        BQ_conn = BQ_connection
        client = bigquery.Client()
        # # TODO: Set project_id to your Google Cloud Platform project ID.
        # # project_id = "my-project"
        # # TODO: Set table_id to the full destination table ID (including the dataset ID).
        # # table_id = 'my_dataset.my_table'
        pandas_gbq.to_gbq(df, table_id, project_id=project_id,if_exists='replace')
        table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows and {} column to {}".format(table.num_rows,len(table.schema), table_id))

    ##################################################
    #### mysql INSERT db into big query ######## USING JOBCONFIG
    async def mysql_insert_to_BQ(connection,query,column,BQ_connection,table_id,set_schema):
        cnx = connection
        cursor = cnx.cursor(buffered=True)
        
        mySql_select_Query = query
        cursor.execute(mySql_select_Query)
        record = cursor.fetchall()

        df = pd.DataFrame(record, columns = column)
        # Convert from pandas to Arrow https://arrow.apache.org/docs/python/pandas.html
        table = pa.Table.from_pandas(df)
        # Convert back to pandas
        #Reducing Memory Use in Table.to_pandas (split_blocks=True, self_destruct=True)
        df_new = table.to_pandas(split_blocks=True, self_destruct=True)
        del table
        # print(df_new)
        
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            schema=set_schema,autodetect=False,write_disposition="WRITE_TRUNCATE",
        )
        job = client.load_table_from_dataframe(
            df_new, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows and {} column to {}".format(table.num_rows,len(table.schema), table_id))
    
   

     ######## RUN STORE PROCEDURE UPSERT #################   
    
    async def run_procedure(connection,query,table_id):
        client =  bigquery.Client()
        try:
            set_query = (query)
            job_config = client.query(set_query)
            table = client.get_table(table_id)  # Make an API request.
            print("Loaded {} rows and {} column to {}".format(table.num_rows,len(table.schema), table_id))
        except BadRequest as e: #bigquery exception bad request
            for e in job.errors:
                print('ERROR: {}'.format(e['message'])) 
    

    ######### Delete table in Big Query ###################
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    async def delete_table(connection,table_id):  
        # Construct a BigQuery client object.
        client =  bigquery.Client()  
        # TODO(developer): Set table_id to the ID of the table to fetch.
        # table_id = 'your-project.your_dataset.your_table'

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        client.delete_table(table_id, not_found_ok=True) # Make an API request.
        print("Deleted table '{}'.".format(table_id))