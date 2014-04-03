CREATE UNIQUE INDEX mesr_threshold_uk
   ON mesr_threshold_t (
      mesr_keyfigure_id
    , mesr_threshold_from
   );