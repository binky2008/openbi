DECLARE
   l_sql_insert_copy   CLOB := stmt.c_sql_insert_copy;
BEGIN
   stmt.prc_set_text_param (
      l_sql_insert_copy
    , 'targetIdentifier'
    , 'SGC_ACCOUNTS_ST1'
   );
   stmt.prc_set_text_param (
      l_sql_insert_copy
    , 'partition'
    , ''
   );
   DBMS_OUTPUT.put_line (l_sql_insert_copy);
END;