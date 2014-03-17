CREATE OR REPLACE VIEW mes_query_v
AS
   SELECT mes_query_id
        , mes_query_code
        , mes_query_name
        , update_date
     FROM mes_query_t;

COMMENT ON TABLE mes_query_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_query_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('mes_query_v'
                              , 'mes_query_v'
                              , TRUE
                              );
END;
/