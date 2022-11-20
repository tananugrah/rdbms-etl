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

    upsert = (
        """
        create or replace procedure `{project_dataset}.upsert2` (
        table_data STRING, 
        table_changes STRING, 
        project_dataset STRING,
        primary_key STRING
        )
        BEGIN
        declare fields STRING;
        declare updates STRING;
        EXECUTE IMMEDIATE (
        "SELECT STRING_AGG(column_name) FROM `"||project_dataset||"`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"||table_data||"'"
                ) INTO fields;
        EXECUTE IMMEDIATE (
        "WITH t AS (SELECT column_name FROM `"||project_dataset||"`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"||table_data||"')
        SELECT STRING_AGG("t."||column_name ||" = "|| "s."||column_name) from t join t as s using(column_name)"
                ) INTO updates;
        EXECUTE IMMEDIATE (
                MERGE `"||project_dataset||"."||table_data||"` T
                USING `"||project_dataset||"."||table_changes||"` S
                    ON T."||primary_key||" = S."||primary_key||"
                WHEN MATCHED AND T."||primary_key||" = S."||primary_key||" THEN 
                    UPDATE SET "||updates||"
                WHEN NOT MATCHED THEN
                    INSERT ("||fields||") VALUES ("||fields||"));
        END
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