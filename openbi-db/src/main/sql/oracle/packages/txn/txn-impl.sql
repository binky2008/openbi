CREATE OR REPLACE PACKAGE BODY txn_taxonomy
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-03-20 09:36:48 +0100 (Di, 20 Mrz 2012) $
    * $Revision: 2482 $
    * $Id: pkg_sys-impl.sql 2482 2012-03-20 08:36:48Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_sys/pkg_sys-impl.sql $
    */
   FUNCTION fct_get_taxonomy_emails (
      p_vc_taxonomy_code   IN   VARCHAR2
    , p_vc_separator       IN   VARCHAR2 DEFAULT ','
   )
      RETURN VARCHAR2
   IS
      l_vc_emails   aux_type.vc_max_plsql;
   BEGIN
      for r_email in (select us.aux_user_email
                        from txn_user_t ut
                           , aux_user_t us
                           , txn_taxonomy_t ta
                       WHERE ut.txn_user_id = us.aux_user_id
                         and ut.txn_taxonomy_id = ta.txn_taxonomy_id
                         AND ta.txn_taxonomy_code = p_vc_taxonomy_code)
      LOOP
         l_vc_emails    := l_vc_emails || r_email.aux_user_email || p_vc_separator;
      END LOOP;

      RETURN RTRIM (l_vc_emails, p_vc_separator);
   END;

   PROCEDURE prc_environment_ins (
      p_vc_environment_code   IN   VARCHAR2
    , p_vc_environment_name   IN   VARCHAR2
    , p_vc_environment_db     IN   VARCHAR2
   )
   IS
   begin
      MERGE INTO txn_environment_t trg
         USING (SELECT p_vc_environment_code AS environment_code
                     , p_vc_environment_name as environment_name
                     , p_vc_environment_db AS environment_db
                  FROM DUAL) src
         ON (trg.txn_environment_code = src.environment_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.txn_environment_name = src.environment_name, trg.txn_environment_db = src.environment_db
         when not matched then
            INSERT (trg.txn_environment_code, trg.txn_environment_name, trg.txn_environment_db)
            VALUES (src.environment_code, src.environment_name, src.environment_db);
      COMMIT;
   END;

   PROCEDURE prc_layer_ins (
      p_vc_layer_code   IN   VARCHAR2
    , p_vc_layer_name   IN   VARCHAR2
   )
   IS
   begin
      MERGE INTO txn_layer_t trg
         USING (SELECT p_vc_layer_code AS layer_code
                     , p_vc_layer_name AS layer_name
                  FROM DUAL) src
         ON (trg.txn_layer_code = src.layer_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.txn_layer_name = src.layer_name
         WHEN NOT MATCHED THEN
            INSERT (trg.txn_layer_code)
            VALUES (src.layer_code);
      COMMIT;
   END;

   PROCEDURE prc_entity_ins (
      p_vc_entity_code   IN   VARCHAR2
    , p_vc_entity_name   IN   VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO txn_entity_t trg
         USING (SELECT p_vc_entity_code AS entity_code
                     , p_vc_entity_name AS entity_name
                  FROM DUAL) src
         ON (trg.txn_entity_code = src.entity_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.txn_entity_name = src.entity_name
         WHEN NOT MATCHED THEN
            INSERT (trg.txn_entity_code)
            VALUES (src.entity_code);
      COMMIT;
   END;

   PROCEDURE prc_taxonomy_ins (
      p_vc_taxonomy_code          IN   VARCHAR2
    , p_vc_taxonomy_name          IN   VARCHAR2
    , p_vc_taxonomy_parent_code   IN   VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO txn_taxonomy_t trg
         USING (SELECT p_vc_taxonomy_code AS taxonomy_code
                     , p_vc_taxonomy_name AS taxonomy_name
                     , txn_taxonomy_id AS taxonomy_parent_id
                  FROM txn_taxonomy_t
                 WHERE txn_taxonomy_code = p_vc_taxonomy_parent_code) src
         ON (trg.txn_taxonomy_code = src.taxonomy_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.txn_taxonomy_name = src.taxonomy_name, trg.txn_taxonomy_parent_id = NVL (src.taxonomy_parent_id, trg.txn_taxonomy_parent_id)
         WHEN NOT MATCHED THEN
            INSERT (trg.txn_taxonomy_code, trg.txn_taxonomy_name, trg.txn_taxonomy_parent_id)
            VALUES (src.taxonomy_code, src.taxonomy_name, src.taxonomy_parent_id);
      COMMIT;
   END;

   PROCEDURE prc_user_ins (
      p_vc_user_code    IN   VARCHAR2
    , p_vc_user_name    IN   VARCHAR2
    , p_vc_user_email   IN   VARCHAR2
   )
   IS
   begin
      MERGE INTO aux_user_t trg
         USING (SELECT p_vc_user_code AS user_code
                     , p_vc_user_name AS user_name
                     , p_vc_user_email AS user_email
                  FROM DUAL) src
         ON (trg.aux_user_code = src.user_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.aux_user_name = NVL (src.user_name, trg.aux_user_name), trg.aux_user_email = NVL (src.user_email, trg.aux_user_email)
         WHEN NOT MATCHED THEN
            INSERT (trg.aux_user_code, trg.aux_user_name, trg.aux_user_email)
            VALUES (src.user_code, src.user_name, src.user_email);
      COMMIT;
   END;

   PROCEDURE prc_user_taxonomy_ins (
      p_vc_user_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   aux_type.vc_obj_plsql := 'PRC_USER_TAXONOMY_INS';
   BEGIN
      aux_log.LOG ('Inserting in sys_user_taxonomy_t', l_vc_prc_name);
      merge into txn_user_t trg
         USING (SELECT aux_user_id
                     , txn_taxonomy_id
                  FROM aux_user_t c
                     , txn_taxonomy_t t
                 WHERE c.aux_user_code = p_vc_user_code
                   and t.txn_taxonomy_code = p_vc_taxonomy_code) src
         ON (    trg.aux_user_id = src.aux_user_id
             AND trg.txn_taxonomy_id = src.txn_taxonomy_id)
         WHEN NOT MATCHED THEN
            insert (trg.aux_user_id, trg.txn_taxonomy_id)
            VALUES (src.aux_user_id, src.txn_taxonomy_id);
      aux_log.LOG (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_user_taxonomy_ins;

   PROCEDURE prc_user_taxonomy_del (
      p_vc_user_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   aux_type.vc_obj_plsql := 'PRC_USER_TAXONOMY_DEL';
   BEGIN
      aux_log.LOG ('Deleting in sys_user_taxonomy_t', l_vc_prc_name);

      delete      txn_user_t
            WHERE aux_user_id = (SELECT aux_user_id
                                   FROM aux_user_t
                                  where aux_user_code = p_vc_user_code)
              AND txn_taxonomy_id = (SELECT txn_taxonomy_id
                                       FROM txn_taxonomy_t
                                      WHERE txn_taxonomy_code = p_vc_taxonomy_code);

      aux_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_user_taxonomy_del;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_sys-impl.sql 2482 2012-03-20 08:36:48Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_sys/pkg_sys-impl.sql $';
END txn_taxonomy;
/

show errors;