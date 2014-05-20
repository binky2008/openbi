CREATE UNIQUE INDEX p#frm#mesr_threshold_uk
   ON p#frm#mesr_threshold_t (
      mesr_keyfigure_id
    , mesr_threshold_from
   );