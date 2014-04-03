CREATE OR REPLACE PACKAGE BODY taxn
AS
   /**
   * $Author: nmarangoni $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   FUNCTION fct_get_taxonomy_emails (
      p_vc_taxonomy_code   IN VARCHAR2
    , p_vc_separator       IN VARCHAR2 DEFAULT ','
   )
      RETURN VARCHAR2
   IS
      l_vc_emails   TYPE.vc_max_plsql;
   BEGIN
      FOR r_email IN (SELECT us.user_email
                        FROM taxn_user_t ut
                           , user_t us
                           , taxn_t ta
                       WHERE ut.user_id = us.user_id
                         AND ut.taxn_id = ta.taxn_id
                         AND ta.taxn_code = p_vc_taxonomy_code) LOOP
         l_vc_emails :=
               l_vc_emails
            || r_email.user_email
            || p_vc_separator;
      END LOOP;

      RETURN RTRIM (
                l_vc_emails
              , p_vc_separator
             );
   END;

   PROCEDURE prc_taxonomy_ins (
      p_vc_taxonomy_code          IN VARCHAR2
    , p_vc_taxonomy_name          IN VARCHAR2
    , p_vc_taxonomy_parent_code   IN VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO taxn_t trg
           USING (SELECT p_vc_taxonomy_code AS taxonomy_code
                       , p_vc_taxonomy_name AS taxonomy_name
                       , taxn_id AS taxonomy_parent_id
                    FROM taxn_t
                   WHERE taxn_code = p_vc_taxonomy_parent_code) src
              ON (trg.taxn_code = src.taxonomy_code)
      WHEN MATCHED THEN
         UPDATE SET trg.taxn_name = src.taxonomy_name
                  , trg.taxn_parent_id = NVL (src.taxonomy_parent_id, trg.taxn_parent_id)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.taxn_code
                     , trg.taxn_name
                     , trg.taxn_parent_id
                    )
             VALUES (
                       src.taxonomy_code
                     , src.taxonomy_name
                     , src.taxonomy_parent_id
                    );

      COMMIT;
   END;

   PROCEDURE prc_user_ins (
      p_vc_user_code    IN VARCHAR2
    , p_vc_user_name    IN VARCHAR2
    , p_vc_user_email   IN VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO user_t trg
           USING (SELECT p_vc_user_code AS user_code
                       , p_vc_user_name AS user_name
                       , p_vc_user_email AS user_email
                    FROM DUAL) src
              ON (trg.user_code = src.user_code)
      WHEN MATCHED THEN
         UPDATE SET trg.user_name = NVL (src.user_name, trg.user_name)
                  , trg.user_email = NVL (src.user_email, trg.user_email)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.user_code
                     , trg.user_name
                     , trg.user_email
                    )
             VALUES (
                       src.user_code
                     , src.user_name
                     , src.user_email
                    );

      COMMIT;
   END;

   PROCEDURE prc_user_taxonomy_ins (
      p_vc_user_code       IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   )
   IS
      l_vc_prc_name   TYPE.vc_obj_plsql := 'PRC_USER_TAXONOMY_INS';
   BEGIN
       trac.log_sub_info (
         l_vc_prc_name
       , 'Inserting in sys_user_taxonomy_t'
      );

      MERGE INTO taxn_user_t trg
           USING (SELECT user_id
                       , taxn_id
                    FROM user_t c
                       , taxn_t t
                   WHERE c.user_code = p_vc_user_code
                     AND t.taxn_code = p_vc_taxonomy_code) src
              ON (trg.user_id = src.user_id
              AND trg.taxn_id = src.taxn_id)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.user_id
                     , trg.taxn_id
                    )
             VALUES (
                       src.user_id
                     , src.taxn_id
                    );

       trac.log_sub_info (
         l_vc_prc_name
       ,    SQL%ROWCOUNT
         || ' rows merged'
      );
      COMMIT;
   END prc_user_taxonomy_ins;

   PROCEDURE prc_user_taxonomy_del (
      p_vc_user_code       IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   )
   IS
      l_vc_prc_name   TYPE.vc_obj_plsql := 'PRC_USER_TAXONOMY_DEL';
   BEGIN
       trac.log_sub_info (
         l_vc_prc_name
       , 'Deleting in sys_user_taxonomy_t'
      );

      DELETE taxn_user_t
       WHERE user_id = (SELECT user_id
                          FROM user_t
                         WHERE user_code = p_vc_user_code)
         AND taxn_id = (SELECT taxn_id
                         FROM taxn_t
                        WHERE taxn_code = p_vc_taxonomy_code);

       trac.log_sub_info (
         l_vc_prc_name
       ,    SQL%ROWCOUNT
         || ' rows deleted'
      );
      COMMIT;
   END prc_user_taxonomy_del;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: $';
   c_body_url := '$HeadURL: $';
END taxn;