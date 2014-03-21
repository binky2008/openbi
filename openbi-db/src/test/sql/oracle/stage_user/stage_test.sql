/* Formatted on 21/03/2014 15:07:32 (QP5 v5.227.12220.39754) */
BEGIN
   TRC.LOG_INFO('test','test');
   stg_meta.prc_source_ins ('SGC',
                                     'SGC',
                                     'SugarCRM',
                                     'STAGE',
                                     'SOURCE01_DATA',
                                     'SOURCE01_INDX',
                                     'STAGE01_DATA',
                                     'STAGE01_INDX',
                                     'TOTALRECALL');
   --
   stg_meta.prc_source_db_ins ('SGC',
                                        'NONE',
                                        'SUGARCRM',
                                        'SUGARCRM');

   stg_meta.prc_object_ins ('SGC', 'ACCOUNTS');
   --
   /*DWHADMIN.STG_META.PRC_COLUMN_IMPORT ('SGC');
   --
   DWHADMIN.STG_META.prc_column_ins ('SGC',
                                     'ACCOUNTS',
                                     'DESCRIPTION',
                                     p_n_column_edwh_flag   => 0);*/
    --
    --DWHADMIN.STG_BUILD.PRC_BUILD_ALL('SGC');
                                     
                                     
                                     
END;