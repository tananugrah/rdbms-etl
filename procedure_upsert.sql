-- define store procedure for upsert data
create or replace procedure `cesatriadata2022.ce_access_layer.upsert` (
    table_data STRING, 
    table_changes STRING, 
    project_dataset STRING,
    primary_key STRING
  )
begin
declare fields STRING;
declare updates STRING;
EXECUTE IMMEDIATE (
     "SELECT STRING_AGG(column_name) FROM `"||project_dataset||"`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"||table_data||"'"
  ) INTO fields;
EXECUTE IMMEDIATE (
    """WITH t AS (SELECT column_name FROM `"""||project_dataset||"""`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"""||table_data||"""')
       SELECT STRING_AGG("t."||column_name ||" = "|| "s."||column_name) from t join t as s using(column_name)"""
  ) INTO updates;
EXECUTE IMMEDIATE """
  MERGE `"""||project_dataset||"""."""||table_data||"""` T
  USING `"""||project_dataset||"""."""||table_changes||"""` S
    ON T."""||primary_key||""" = S."""||primary_key||"""
  WHEN MATCHED AND T."""||primary_key||""" = S."""||primary_key||""" THEN 
    UPDATE SET """||updates||"""
  WHEN NOT MATCHED THEN
    INSERT ("""||fields||""") VALUES ("""||fields||""")""";
end;