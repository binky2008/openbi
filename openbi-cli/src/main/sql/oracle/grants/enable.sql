BEGIN
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-23 17:56:23 +0200 (Mo, 23 Jul 2012) $
    * $Revision: 3021 $
    * $Id: enable.sql 3021 2012-07-23 15:56:23Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/grants/enable.sql $
    */
   -- Grant select rights on every table and view to the read-only user
   FOR r_obj IN (SELECT   object_name
                     FROM user_objects
                    WHERE object_type IN ('TABLE', 'VIEW')
                 ORDER BY object_type)
   LOOP
      BEGIN
         EXECUTE IMMEDIATE 'GRANT SELECT ON ' || r_obj.object_name || ' TO EDWH_ADM_READ';
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;
   END LOOP;

   BEGIN
      -- Enable users to the use of the single components
      -- Stage components
      pkg_enable.prc_enable_utl ('STAGE');
      pkg_enable.prc_enable_qc ('STAGE');
      pkg_enable.prc_enable_stage ('STAGE');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      -- Core and QC components
      pkg_enable.prc_enable_utl ('EDWH_CL');
      pkg_enable.prc_enable_core ('EDWH_CL');
      pkg_enable.prc_enable_qc ('EDWH_CL');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      -- QC components
      pkg_enable.prc_enable_utl ('EDWH_QC');
      pkg_enable.prc_enable_qc ('EDWH_QC');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      pkg_enable.prc_enable_utl ('DM_AR');
      pkg_enable.prc_enable_core ('DM_AR');
      pkg_enable.prc_enable_qc ('DM_AR');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   -- Enable users to the use of the qc-framework
   BEGIN
      pkg_enable.prc_enable_utl ('CVEIT');
      pkg_enable.prc_enable_qc ('CVEIT');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      pkg_enable.prc_enable_utl ('UMAHN');
      pkg_enable.prc_enable_qc ('UMAHN');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      pkg_enable.prc_enable_utl ('NMARANGO');
      pkg_enable.prc_enable_qc ('NMARANGO');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      pkg_enable.prc_enable_utl ('AMARCHENKO');
      pkg_enable.prc_enable_qc ('AMARCHENKO');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   BEGIN
      pkg_enable.prc_enable_utl ('HHEHENBE');
      pkg_enable.prc_enable_qc ('HHEHENBE');
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;
END;
/