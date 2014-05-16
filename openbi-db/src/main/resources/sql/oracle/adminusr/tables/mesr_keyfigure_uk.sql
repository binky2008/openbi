CREATE UNIQUE INDEX mesr_keyfigure_uk
   ON mesr_keyfigure_t (
      mesr_query_id
    , mesr_keyfigure_code
   );