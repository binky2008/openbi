CREATE UNIQUE INDEX p#frm#mesr_taxn_uk
   ON p#frm#mesr_taxn_t (
      mesr_query_id
    , taxn_id
   );