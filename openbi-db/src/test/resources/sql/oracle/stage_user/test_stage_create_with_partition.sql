BEGIN
   ROLLBACK;
   stag_meta.prc_source_ins (
      'TST'
    , 'TST'
    , 'Test'
    , 'DWHSTAGE'
    , 'USERS'
    , 'USERS'
    , 'USERS'
    , 'USERS'
   );
   --
   stag_meta.prc_source_db_ins (
      'TST'
    , 'NONE'
    , 'TEST'
    , 'TEST'
   );
   stag_meta.prc_object_ins (
      'TST'
    , 'BIGTABLE'
    , p_vc_partition_clause   => 'substr( col_pk,-1)'
   );
   stag_meta.prc_object_ins (
      'TST'
    , 'SMALLTABLE'
    , p_vc_hist_flag   => 1
   );
   --
   stag_meta.prc_column_ins (
      'TST'
    , 'SMALLTABLE'
    , 'COL_OTHERTEXT'
    , p_n_column_hist_flag   => 0
   );
   --
   dwhadmin.stag_meta.prc_column_import_from_source ('TST');
   --
   dwhadmin.stag_build.prc_build_all ('TST');
   ROLLBACK;
END;