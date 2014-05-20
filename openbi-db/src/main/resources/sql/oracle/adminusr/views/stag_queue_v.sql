CREATE OR REPLACE VIEW p#frm#stag_queue_v
AS
   SELECT stag_queue_id
        , stag_queue_code
        , stag_queue_name
     FROM p#frm#stag_queue_t q;