BEGIN
   ROLLBACK;
   --
   stag_build.prc_upgrade_hist (
      'TST'
    , 'SMALLTABLE'
   );
   --
   ROLLBACK;
END;
/