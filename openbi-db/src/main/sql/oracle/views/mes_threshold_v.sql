CREATE OR REPLACE VIEW mes_threshold_v
AS
   SELECT   q.mes_query_code
          , q.mes_query_name
          , q.mes_query_sql
          , k.mes_keyfigure_code
          , k.mes_keyfigure_name
          , t.mes_threshold_type
          , t.mes_threshold_from
          , t.mes_threshold_to
          , t.mes_threshold_min
          , t.mes_threshold_max
       FROM mes_query_t q
          , mes_keyfigure_t k
          , mes_threshold_t t
      WHERE q.mes_query_id = k.mes_query_id
        AND k.mes_keyfigure_id = t.mes_keyfigure_id
   ORDER BY q.mes_query_id DESC
          , k.mes_keyfigure_id DESC
          , t.mes_threshold_id DESC;

COMMENT ON TABLE mes_threshold_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_threshold_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('mes_threshold_v'
                                 , 'mes_threshold_v'
                                 , TRUE
                                  );
END;
/