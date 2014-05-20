DECLARE
   l_sql_insert_copy   CLOB := stmt.c_sql_insert_copy;
BEGIN
   stmt.prc_set_text_param (
      l_sql_insert_copy
    , 'targetIdentifier'
    , 'SGC_ACCOUNTS_STG'
   );
   stmt.prc_set_text_param (
      l_sql_insert_copy
    , 'partition'
    , ''
   );
   stmt.prc_set_text_param (
      l_sql_insert_copy
    , 'filterClause'
    , ''
   );
   stmt.prc_set_text_param (
      l_sql_insert_copy
    , 'sourceIdentifier'
    , 'SUGARCRM.ACCOUNTS@SUGARCRM'
   );
   DBMS_OUTPUT.put_line (l_sql_insert_copy);
END;