import os
import sys
sys.path.append(os.getcwd())
import time

import asyncio
import asyncpg
import pandas as pd

from src import Connection
from TransactionData import DataOperationBase
from query.SqlCommand import QueryServices

from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound

start_time = time.time()
loop = asyncio.new_event_loop()


if __name__ == '__main__':
    #### set connection postgre ######
    conn_pg = loop.run_until_complete(
        Connection.DBClient.ConnectionDB()
    )
    ### set connection mysql ######
    conn_mysql = loop.run_until_complete(
        Connection.DBClient.ConnectionMysql()
    )

    # conn_BQ = loop.run_until_complete(
    #     Connection.DBClient.ConnectionBQ()
    # )


    # ##### extract data postgre db table actor #######
    # actor = loop.run_until_complete(
    #     DataOperationBase.Extract(
    #         connection = conn_pg,
    #         query = QueryServices.select.format(
    #             Column = 'actor_id, first_name, last_name, last_update',
    #             Table = 'actor',
    #             FilterColumn = ""
    #         )   
    #     )
    # )
    # df = pd.DataFrame(data = actor, columns = ['actor_id', 'first_name',\
    #                                             'last_name','last_update'])
    # print(df)

    # ##### extract data mysql db table customers #######
    # customers = loop.run_until_complete(
    #     DataOperationBase.Extract_Mysql(
    #         connection = conn_mysql,
    #         query = QueryServices.select.format(
    #             Column = 'customerNumber,customerName,contactLastName',
    #             Table = 'customers',
    #             FilterColumn = ""
    #         )
    #     )
    # )
    # # print(customers)
    # df = pd.DataFrame(data = customers, columns = ['customerNumber','customerName','contactLastName'])
    # print(df)

    # # ######## bigquery create dataset #######
    # BQ_create_dataset_1 = loop.run_until_complete(
    #                     DataOperationBase.create_dataset(
    #                         connection = conn_BQ,
    #                         dataset_id="learning-data-369615.data_source_pg"
    #                     ))

    # BQ_create_dataset_2 = loop.run_until_complete(
    #                     DataOperationBase.create_dataset(
    #                         connection = conn_BQ,
    #                         dataset_id="learning-data-369615.data_source_mysql"
    #                     ))

    # ###### big query create table actor from source postgre db ######
    # #create table if exist
    # BQ_create_table = loop.run_until_complete(
    #                     DataOperationBase.bq_create_table(
    #                     connection = conn_BQ,
    #                     dataset = 'data_source_pg',
    #                     table = 'PROD_actor',
    #                     set_schema = [
    #                                 bigquery.SchemaField('actor_id', 'INTEGER', mode='REQUIRED'),
    #                                 bigquery.SchemaField('first_name', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('last_name', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('last_update', 'TIMESTAMP', mode='REQUIRED')
    #                                 ]
    #     )
    # )

    # # ####### big query create table as select actor from source postgre db ######
    # BQ_create_table_ctas = loop.run_until_complete(
    #                         DataOperationBase.bq_create_table_ctas(
    #                         connection = conn_BQ,
    #                         dataset = 'data_source_pg',
    #                         new_table = 'STAG_actor',
    #                         query = QueryServices.create_table_ctas.format(
    #                             New_Table = 'learning-data-engineering-1.data_source_pg.STAG_actor',
    #                             Column = '*',
    #                             Table = 'learning-data-engineering-1.data_source_pg.PROD_actor'
    #                         )
    #                     )
    #                 )
    
    # # ####### big query insert data to table STAG ########
    # #  # #https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
    # BQ_Insert_STAG_actor = loop.run_until_complete(
    #     DataOperationBase.insert_to_BQ(
    #         connection = conn_pg,
    #         query = QueryServices.select.format(
    #             Column = 'actor_id, first_name, last_name,last_update',
    #             Table = 'actor',
    #             FilterColumn = ''
    #         ),
    #         column = ('actor_id', 'first_name', 'last_name','last_update'),
    #         BQ_connection = conn_BQ,
    #         project_id = "learning-data-engineering-1",
    #         table_id = "data_source_pg.STAG_actor",
            
    #     )
    # )
    #  ####### big query create table address from source postgre db ######
    # #create table if exist
    # BQ_create_table_address = loop.run_until_complete(
    #                     DataOperationBase.bq_create_table(
    #                     connection = conn_BQ,
    #                     dataset = 'data_source_pg',
    #                     table = 'PROD_address',
    #                     set_schema = [
    #                                 bigquery.SchemaField('address_id', 'INTEGER', mode='REQUIRED'),
    #                                 bigquery.SchemaField('address', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('address2', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('district', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('city_id', 'INTEGER', mode='REQUIRED'),
    #                                 bigquery.SchemaField('postal_code', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('phone', 'STRING', mode='REQUIRED'),
    #                                 bigquery.SchemaField('last_update', 'DATETIME', mode='REQUIRED')
    #                                 ]
    #     )
    # )
    # ####### big query create table as select address from source postgre db ######
    # BQ_create_table_ctas_address = loop.run_until_complete(
    #                         DataOperationBase.bq_create_table_ctas(
    #                         connection = conn_BQ,
    #                         dataset = 'data_source_pg',
    #                         new_table = 'STAG_address',
    #                         query = QueryServices.create_table_ctas.format(
    #                             New_Table = 'learning-data-engineering-1.data_source_pg.STAG_address',
    #                             Column = '*',
    #                             Table = 'learning-data-engineering-1.data_source_pg.PROD_address'
    #                         )
    #                     )
    #                 )

    # # ####### big query insert data to table STAG ########
    # #  # #https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
    # BQ_Insert_STAG_address = loop.run_until_complete(
    #     DataOperationBase.insert_to_BQ(
    #         connection = conn_pg,
            
    #         query = QueryServices.select.format(
    #             Column = 'address_id,address,address2,district,city_id,postal_code,phone,last_update',
    #             Table = 'address',
    #             FilterColumn = ''
    #         ),
    #         column = ('address_id','address','address2','district','city_id','postal_code','phone','last_update'),
    #         BQ_connection = conn_BQ,
    #         project_id = "learning-data-engineering-1",
    #         table_id = "data_source_pg.STAG_address",     
    #     )
#     # )
################################################################################################################
    # ###### big query create table customers from source mysql db ######
    # #create table if exist
    # BQ_create_table = loop.run_until_complete(
    #                     DataOperationBase.bq_create_table(
    #                     connection = conn_BQ,
    #                     dataset = 'data_source_mysql',
    #                     table = 'PROD_customers',
    #                     set_schema = [
    #                                 bigquery.SchemaField('customerNumber', 'INTEGER', mode='REQUIRED'),
    #                                 bigquery.SchemaField('customerName', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('contactLastName', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('contactFirstName', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('phone', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('addressLine1', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('addressLine2', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('city', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('state', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('postalCode', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('country', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('salesRepEmployeeNumber', 'INTEGER', mode='Nullable'),
    #                                 bigquery.SchemaField('creditLimit', 'NUMERIC', mode='Nullable')
    #                                 ]
    #     )
    # )
    #  ####### big query create table as select address from source mysql db ######
    # BQ_create_table_ctas_customers = loop.run_until_complete(
    #                         DataOperationBase.bq_create_table_ctas(
    #                         connection = conn_BQ,
    #                         dataset = 'data_source_mysql',
    #                         new_table = 'STAG_customers',
    #                         query = QueryServices.create_table_ctas.format(
    #                             New_Table = 'learning-data-369615.data_source_mysql.STAG_customers',
    #                             Column = 'customerNumber,customerName,contactLastName,contactFirstName,phone,addressLine1,addressLine2,city,state,postalCode,country,salesRepEmployeeNumber,creditLimit',
    #                             Table = 'learning-data-369615.data_source_mysql.PROD_customers'
    #                         )
    #                     )
    #                 )

    # ####### big query insert data to table STAG from mysql ########
    #  # #https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
    # BQ_Insert_STAG_customers = loop.run_until_complete(
    #     DataOperationBase.mysql_insert_to_BQ(
    #         connection = conn_mysql,
    #         query = QueryServices.select.format(
    #             Column = 'customerNumber,customerName,contactLastName,contactFirstName,phone,addressLine1,addressLine2,city,state,postalCode,country,salesRepEmployeeNumber,creditLimit',
    #             Table = 'customers',
    #             FilterColumn = ''
    #         ),
    #         column = ('customerNumber','customerName','contactLastName','contactFirstName','phone','addressLine1','addressLine2','city','state','postalCode','country','salesRepEmployeeNumber','creditLimit'),
    #         BQ_connection = conn_BQ,
    #         set_schema = [
    #                     bigquery.SchemaField('customerNumber', 'INTEGER'),
    #                     bigquery.SchemaField('customerName', 'STRING'),
    #                     bigquery.SchemaField('contactLastName', 'STRING'),
    #                     bigquery.SchemaField('contactFirstName', 'STRING'),
    #                     bigquery.SchemaField('phone', 'STRING'),
    #                     bigquery.SchemaField('addressLine1', 'STRING'),
    #                     bigquery.SchemaField('addressLine2', 'STRING'),
    #                     bigquery.SchemaField('city', 'STRING'),
    #                     bigquery.SchemaField('state', 'STRING'),
    #                     bigquery.SchemaField('postalCode', 'STRING'),
    #                     bigquery.SchemaField('country', 'STRING'),
    #                     bigquery.SchemaField('salesRepEmployeeNumber', 'INTEGER'),
    #                     bigquery.SchemaField('creditLimit', 'NUMERIC')
    #         ],
            
    #         # project_id = "learning-data-engineering-1",
    #         table_id = "data_source_mysql.STAG_customers",     
    #     )
    # )
    #############################################################################################################
    # ##### big query create table employees from source mysql db ######
    # #create table if exist
    # BQ_create_table = loop.run_until_complete(
    #                     DataOperationBase.bq_create_table(
    #                     connection = conn_BQ,
    #                     dataset = 'data_source_mysql',
    #                     table = 'PROD_employees',         
    #                     set_schema = [
    #                                 bigquery.SchemaField('employeeNumber', 'INTEGER', mode='REQUIRED'),
    #                                 bigquery.SchemaField('lastName', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('firstName', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('extension', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('email', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('officeCode', 'STRING', mode='Nullable'),
    #                                 bigquery.SchemaField('reportsTo', 'INTEGER', mode='Nullable'),
    #                                 bigquery.SchemaField('jobTitle', 'STRING', mode='Nullable'),
    #                                 ]
    #     )
    # )

    #  ####### big query create table as select from source mysql db ######
    # BQ_create_table_ctas_employees = loop.run_until_complete(
    #                         DataOperationBase.bq_create_table_ctas(
    #                         connection = conn_BQ,
    #                         dataset = 'data_source_mysql',
    #                         new_table = 'STAG_employees',
    #                         query = QueryServices.create_table_ctas.format(
    #                             New_Table = 'learning-data-369615.data_source_mysql.STAG_employees',
    #                             Column = 'employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle',
    #                             Table = 'learning-data-369615.data_source_mysql.PROD_employees'
    #                         )
    #                     )
    #                 )
    
    # ####### big query insert data employees to table STAG from mysql ######## using JOBCONFIG
    #  # #https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
    # BQ_Insert_STAG_employees = loop.run_until_complete(
    #     DataOperationBase.mysql_insert_to_BQ(
    #         connection = conn_mysql,
            
    #         query = QueryServices.select.format(
    #             Column = 'employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle',
    #             Table = 'employees',
    #             FilterColumn = ''
    #         ),
    #         column = ('employeeNumber','lastName','firstName','extension','email','officeCode','reportsTo','jobTitle'),
    #         BQ_connection = conn_BQ,
    #         set_schema = [
    #                     bigquery.SchemaField('employeeNumber', 'INTEGER'),
    #                     bigquery.SchemaField('lastName', 'STRING'),
    #                     bigquery.SchemaField('firstName', 'STRING'),
    #                     bigquery.SchemaField('extension', 'STRING'),
    #                     bigquery.SchemaField('email', 'STRING'),
    #                     bigquery.SchemaField('officeCode', 'STRING'),
    #                     bigquery.SchemaField('reportsTo', 'INTEGER'),
    #                     bigquery.SchemaField('jobTitle', 'STRING')
                        
    #         ],
    #         # project_id = "learning-data-369615",
    #         table_id = "data_source_mysql.STAG_employees",     
    #     )
    # )
    
    # create_sp_upsert = loop.run_until_complete(
    #     DataOperationBase.create_sp_upsert(
    #         connection = conn_BQ,
    #         query = QueryServices.upsert.format(
    #             project_dataset = 'learning-data-engineering-1.data_source_pg'
                
    #         )   
    #     )
    # )
    # ######################### Run Store Procedure to Upsert data #########################
    # run_procedure_upsert_actor = loop.run_until_complete(
    #     DataOperationBase.run_procedure(
    #         connection = conn_BQ,
    #         query = QueryServices.run_procedure.format(
    #             table_data = 'PROD_employees',
    #             table_change = 'STAG_employees',
    #             project_dataset = 'learning-data-369615.data_source_mysql',
    #             primary_key = 'employeeNumber'
    #         ),
    #         table_id = 'learning-data-369615.data_source_mysql.PROD_employees'
    #     )
    # )
    # ########################## Delete table STAG after upsert into table PROD ##################
    # delete_table_STAG_actor = loop.run_until_complete(
    #     DataOperationBase.delete_table(
    #         connection = conn_BQ,
    #         table_id = 'learning-data-engineering-1.data_source_pg.STAG_actor'
    #     )
    # )

    # # ######################### MYSQL Run Store Procedure to Upsert data #########################
    # ############## upsert customers #####################
    # run_procedure_upsert_customer = loop.run_until_complete(
    #     DataOperationBase.run_procedure(
    #         connection = conn_BQ,
    #         table_id = "data_source_mysql.PROD_customers",
    #         query = QueryServices.run_procedure.format(
    #                 project_dataset = 'learning-data-369615.data_source_mysql',
    #                 procedure = 'upsert',
    #                 table_data = 'PROD_customers',
    #                 table_change = 'STAG_customers',
    #                 primary_key = 'customerNumber'
    #         )
    #     )
    # )
    # #  ############### upsert employees #####################
    # run_procedure_upsert_employees = loop.run_until_complete(
    #     DataOperationBase.run_procedure(
    #         connection = conn_BQ,
    #         query = QueryServices.run_procedure.format(
    #             project_dataset = 'learning-data-369615.data_source_mysql',
    #             procedure = 'upsert',
    #             table_data = 'PROD_employees',
    #             table_change = 'STAG_employees',
    #             primary_key = 'employeeNumber'
    #         ),
    #         table_id = "data_source_mysql.PROD_employees"
    #     )
    # )




