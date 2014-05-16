CREATE UNIQUE INDEX mesr_taxn_uk
   ON mesr_taxn_t (
      mesr_query_id
    , taxn_id
   );