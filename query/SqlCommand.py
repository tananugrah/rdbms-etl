class QueryServices :
    select = (
        """
         select 
            {Column}
        from {Table}
        {FilterColumn};
        
        """
    )
    createDB = (
        """
        CREATE DATABASE {DB_name};

        """
        )

    create_table_ctas = (
        """
        create table {New_Table} as
                     (select {Column} FROM {Table}); 
        """
    )

    run_procedure = (
        """
        declare fields STRING;
        declare updates STRING;
        call `{project_dataset}.upsert` (
        '{table_data}',
        '{table_change}',
        '{project_dataset}.',
        '{primary_key}'
        )
        """
   )