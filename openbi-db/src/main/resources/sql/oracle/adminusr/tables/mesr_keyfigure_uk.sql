CREATE UNIQUE INDEX p#frm#mesr_keyfigure_uk
   ON p#frm#mesr_keyfigure_t (
      mesr_query_id
    , mesr_keyfigure_code
   );