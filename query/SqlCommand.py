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