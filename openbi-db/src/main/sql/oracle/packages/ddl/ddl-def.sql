CREATE OR REPLACE PACKAGE aux_ddl AUTHID CURRENT_USER AS
  /**
  * Templates for standard ddls
  * APIs to construct list of columns and column definitions
  * $Author: nmarangoni $
  * $Date: 2012-06-08 15:34:39 +0200 (Fr, 08 Jun 2012) $
  * $Revision: 2858 $
  * $Id: $
  * $HeadURL: $
  */

  /**
  * Package spec version string.
  */
  c_spec_version CONSTANT VARCHAR2(1024) := '$Id: $';
  /**
  * Package spec repository URL.
  */
  c_spec_url CONSTANT VARCHAR2(1024) := '$HeadURL: $';
  /**
  * Package body version string.
  */
  c_body_version VARCHAR2(1024);
  /**
  * Package body repository URL.
  */
  c_body_url VARCHAR2(1024);
  /**
  *
  */
  g_vc_src_obj_owner  aux_type.vc_obj_plsql;
  g_vc_src_obj_dblink aux_type.vc_obj_plsql;
  c_name_entity_tab      aux_type.vc_obj_plsql := '#entityName#_T';
  c_name_entity_cdc      aux_type.vc_obj_plsql := '#entityName#_CDC';
  c_name_entity_bkp      aux_type.vc_obj_plsql := '#entityName#_BKP';
  c_name_entity_cbk      aux_type.vc_obj_plsql := '#entityName#_CBK';
  c_name_entity_seq      aux_type.vc_obj_plsql := '#entityName#_SEQ';
  c_name_entity_id       aux_type.vc_obj_plsql := '#entityName#_ID';
  c_name_entity_pk       aux_type.vc_obj_plsql := '#entityName#_PK';
  /**
  * Generic metadata retrieval statements
  */
  -- PL/SQL block to store metadata in a tmp table.
  c_sql_import_metadata aux_type.vc_max_plsql := 'BEGIN
		DELETE #targetObject#;
			
		INSERT INTO #targetObject# #targetColumns#
					#sourceSelect#;
		COMMIT;
	END;';
  c_sql_tab_part        aux_type.vc_max_plsql := 'SELECT COUNT (*)
  FROM all_tab_partitions#dblink#
 WHERE table_owner = :ow
   AND table_name = :tb';
  -- Code token to retrieve all columns of an object and their position inside the pk.
  -- If the object is a view, try to detect PK information from an underlying table.
  -- Works for both remote and local tables.
  c_sql_obj_col_all aux_type.vc_max_plsql := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:p))
     AND table_name = TRIM(UPPER(:p))';
  -- Code token to retrieve pk columns of an object and their position inside the pk.
  -- If the object is a view, try to detect PK information from an underlying table.
  -- Works for both remote and local tables.
  c_sql_obj_col_pk aux_type.vc_max_plsql := 'SELECT tb.object_owner
					 , tb.object_name
					 , cc.column_name
					 , cc.position
				 FROM (SELECT object_owner
								, object_name
								, table_owner
								, table_name
							FROM (SELECT o.owner AS object_owner
										  , o.object_name
										  , CASE
												 WHEN o.object_type = ''VIEW''
													 THEN d.referenced_owner
												 ELSE o.owner
											 END AS table_owner
										  , CASE
												 WHEN o.object_type = ''VIEW''
													 THEN d.referenced_name
												 ELSE o.object_name
											 END AS table_name
										  , COUNT (*) over (PARTITION BY o.owner, o.object_name) AS referenced_cnt
									  FROM all_objects#dblink# o
										  , all_dependencies#dblink# d
									 WHERE o.owner = d.owner(+)
										AND o.object_name = d.name(+)
										AND d.referenced_type(+) = ''TABLE''
										AND o.object_type in (''TABLE'',''VIEW'',''MATERIALIZED VIEW''))
						  WHERE referenced_cnt = 1) tb
					 , all_constraints#dblink# co
					 , all_cons_columns#dblink# cc
				WHERE co.owner = tb.table_owner
				  AND co.table_name = tb.table_name
				  AND co.owner = cc.owner
				  AND co.table_name = cc.table_name
				  AND co.constraint_name = cc.constraint_name
				  AND co.constraint_type = ''P''';
  -- Code token to retrieve pk columns of an object and their position inside the pk.
  -- If the object is a view, it doesn't try to detect PK from dependencies.
  -- Works for both remote and local tables.
  c_sql_obj_col_pk_nodep aux_type.vc_max_plsql := 'SELECT co.owner AS object_owner
                       , co.table_name AS object_name
                       , cc.column_name
                       , cc.position
                    FROM all_constraints#dblink# co
                       , all_cons_columns#dblink# cc
                   WHERE co.owner = cc.owner
                     AND co.table_name = cc.table_name
                     AND co.constraint_name = cc.constraint_name
                     AND co.constraint_type = ''P''';
  -- Get column properties for an object.
  -- Works for both remote and local tables.
  c_sql_col_def aux_type.vc_max_plsql := 'SELECT tc.column_id
	   , tc.column_name
	   , cm.comments
       , tc.data_type ||
         CASE
            WHEN tc.data_type IN (''NUMBER'')
             AND tc.data_precision IS NOT NULL
             AND tc.data_scale IS NOT NULL
               THEN '' ('' || tc.data_precision || '','' || tc.data_scale || '')''
            WHEN tc.data_type LIKE (''%CHAR%'')
               THEN '' ('' || tc.char_length || '')''
         END AS column_def
		 , cs.position AS pk_position
    FROM all_tab_columns#dblink# tc
       , all_col_comments#dblink# cm 
	   , (#sql_obj_pk#) cs
   WHERE tc.owner = cm.owner(+)
     AND tc.table_name = cm.table_name(+)
     AND tc.column_name = cm.column_name(+)
	 AND tc.owner = cs.object_owner(+)
     AND tc.table_name = cs.object_name(+)
     AND tc.column_name = cs.column_name(+)
	 AND tc.owner = TRIM(UPPER(:ow))
     AND tc.table_name = TRIM(UPPER(:tb))
ORDER BY tc.column_id';
  -- Get all columns for a given obejct.
  -- Works for both remote and local tables.
  c_sql_col_all aux_type.vc_max_plsql := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:ow))
     AND table_name = TRIM(UPPER(:tb))
ORDER BY column_id';
  -- Get all pk columns for a given obejct.
  -- If the object is a view, try to detect PK information from an underlying table.
  -- Works for both remote and local tables.
  c_sql_col_pk aux_type.vc_max_plsql := 'SELECT column_name
    FROM (#sql_obj_pk#)
	WHERE object_owner = TRIM(UPPER(:ow))
     AND object_name = TRIM(UPPER(:tb))
ORDER BY position';
  -- Get all non pk columns for a given obejct.
  -- If the object is a view, try to detect PK information from an underlying table.
  -- Works for both remote and local tables.
  c_sql_col_npk aux_type.vc_max_plsql := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:ow))
     AND table_name = TRIM(UPPER(:tb))
  MINUS
  SELECT column_name
    FROM (#sql_obj_pk#)
	WHERE object_owner = TRIM(UPPER(:ow))
     AND object_name = TRIM(UPPER(:tb))';
  -- Get all columns 2 given obejcts have in common.
  c_sql_col_common_all aux_type.vc_max_plsql := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:p))
     AND table_name = TRIM(UPPER(:p))
  INTERSECT
  SELECT column_name
    FROM all_tab_columns
   WHERE owner = TRIM(UPPER(:ow))
     AND table_name = TRIM(UPPER(:tb))';
  -- Get all non-pk columns 2 given obejcts have in common.
  c_sql_col_common_npk aux_type.vc_max_plsql := '(SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:ow1))
     AND table_name = TRIM(UPPER(:tb1))
  MINUS
  SELECT column_name
    FROM (#sql_obj_pk#)
   WHERE object_owner = TRIM(UPPER(:ow1))
     AND object_name = TRIM(UPPER(:tb1)))
  INTERSECT
 (SELECT column_name
    FROM all_tab_columns
   WHERE owner = TRIM(UPPER(:ow2))
     AND table_name = TRIM(UPPER(:tb2))
  MINUS
  SELECT cc.column_name
    FROM all_constraints co
	   , all_cons_columns cc
   WHERE co.owner = cc.owner
     AND co.table_name = cc.table_name
     AND co.constraint_name = cc.constraint_name
     AND co.constraint_type = ''P''
     AND co.owner = TRIM(UPPER(:ow2))
     AND co.table_name = TRIM(UPPER(:tb2))
  MINUS
  SELECT column_name
	FROM all_part_key_columns
   WHERE owner = TRIM(UPPER(:ow2))
     AND name = TRIM(UPPER(:tb2)))';
  -- Get table comments
  c_sql_tab_comm aux_type.vc_max_plsql := 'SELECT comments
  FROM all_tab_comments#dblink#
 WHERE owner = TRIM(UPPER(:ow))
   AND table_name = TRIM(UPPER(:tb))';
  -- Template to create a standard entity table.
  c_template_entity_tab clob := 'CREATE TABLE #entityTable# (
		 #entityId# number
	 , #columnDefinitionList#
	 , session_user VARCHAR2(100) DEFAULT SYS_CONTEXT (''USERENV'', ''SESSION_USER'')
	 , os_user VARCHAR2(100) DEFAULT SYS_CONTEXT (''USERENV'', ''OS_USER'')
	 , create_date DATE DEFAULT SYSDATE
	 , update_date DATE DEFAULT SYSDATE
	 , CONSTRAINT #entityPK# PRIMARY KEY (#entityId#)
	)';
  -- Template to create a cdc table for a standard entity table.
  c_template_entity_cdc CLOB := 'CREATE TABLE #entityCDC# (
		#entityId# NUMBER
	 , #columnDefinitionList#
	 , session_user VARCHAR2(100) DEFAULT SYS_CONTEXT (''USERENV'', ''SESSION_USER'')
	 , os_user VARCHAR2(100) DEFAULT SYS_CONTEXT (''USERENV'', ''OS_USER'')
	 , create_date DATE DEFAULT SYSDATE
	 , update_date DATE DEFAULT SYSDATE
   , dml_operation VARCHAR2(100) DEFAULT ''INSERT''
	 , dml_date DATE DEFAULT SYSDATE
	)';
  -- Template to create a cdc trigger for a standard entity table
  c_template_entity_trg_cdc CLOB := 'CREATE OR REPLACE TRIGGER #entityName#_tdc
   AFTER INSERT OR UPDATE OR DELETE
   ON #entityTable#
   FOR EACH ROW
DECLARE
   l_vc_operation          VARCHAR2 (10);
BEGIN

   IF INSERTING
   THEN
      l_vc_operation        := ''INSERT'';
   ELSIF UPDATING
   THEN
      l_vc_operation        := ''UPDATE'';
   ELSIF DELETING
   THEN
      l_vc_operation        := ''DELETE'';
   END IF;
   
   -- Insert record into audit table
   CASE
      WHEN l_vc_operation IN (''INSERT'', ''UPDATE'')
      THEN
         INSERT INTO #entityCDC#
                     (#columnList#
                    , dml_operation
                     )
              VALUES (#columnListNew#
                    , l_vc_operation
                     );
      WHEN l_vc_operation IN (''DELETE'')
      THEN
         INSERT INTO #entityCDC#
                     (#columnList#
                    , dml_operation
                     )
              VALUES (#columnListOld#
                    , l_vc_operation
                     );
   END CASE;
END;';
  -- Template for the name of a sequence related to a standard entity table.
  c_template_entity_seq CLOB := 'CREATE SEQUENCE #entitySequence#';
  -- Template for the insert trigger related to a standard entity table.
  c_template_entity_trg_ins CLOB := 'CREATE OR REPLACE TRIGGER #entityName#_tbi BEFORE
	INSERT
		ON #entityTable# FOR EACH ROW
BEGIN
	IF :NEW.#entityId# IS NULL THEN
		SELECT
			#entitySequence#.nextval
		INTO
			:NEW.#entityId#
		FROM
			DUAL;
	END IF;
END;';
  -- Template for the update trigger related to a standard entity table.
  c_template_entity_trg_upd CLOB := 'CREATE OR REPLACE TRIGGER #entityName#_tbu BEFORE
	UPDATE
		ON #entityTable# FOR EACH ROW
BEGIN
	:NEW.update_date := SYSDATE;
END;';
  /**
  * Package head code template
  */
  c_template_pkg_head CLOB := 'CREATE OR REPLACE PACKAGE #pkgName# AS
/**
 * This package has been dynamically generated by the
 * ETL framework code generator
 *
 *
 * Version of the framework packages:
#generatingVersion#
 *
 */
   #varList#
   #prcList#

END #pkgName#;';
  /**
  * Package body code template
  */
  c_template_pkg_body CLOB := 'CREATE OR REPLACE PACKAGE BODY #pkgName# AS
   #varList#
   #prcList#
END #pkgName#;';
  /**
  * Procedure head code template
  */
  c_template_prc_head CLOB := '
   PROCEDURE #prcName# (
      #prcParameters#);';
  /**
  * Procedure body code template
  */
  c_template_prc_body CLOB := '
   PROCEDURE #prcName# (
      #prcParameters#) IS
      l_vc_prc_name    aux_type.vc_obj_plsql := ''#prcName#'';
      l_d_start        DATE                  	 := SYSDATE;
	    l_n_gui		   NUMBER							 := NULL;
	    l_n_step_no	   NUMBER;
      l_n_result       NUMBER;
      l_n_stat_id      NUMBER;
	    l_b_ok		   BOOLEAN;
      #varList#
   BEGIN
      #prcInitialize#
   
      aux_log.log (''Start'', l_vc_prc_name);
      
      #prcBody#
      
      aux_log.log (''Finish'', l_vc_prc_name);
		
      #prcFinalize#
		
   EXCEPTION
      WHEN OTHERS THEN
         aux_log.log (SQLERRM, l_vc_prc_name, aux_log.gc_error);
         #exceptionHandling#
         RAISE;
   END #prcName#;';
  /**
  * Function head code template
  */
  c_template_fct_head CLOB := '
   FUNCTION #prcName# (
      #prcParameters#)
   RETURN #returnType#;';
  /**
  * Function body code template
  */
  c_template_fct_body CLOB := '
   FUNCTION #prcName# (
      #prcParameters#)
   RETURN #returnType# IS
      l_vc_prc_name     aux_type.vc_obj_plsql := ''#prcName#'';
      #varList#
   BEGIN
      #prcInitialize#
      
      aux_log.log (''Start'',l_vc_prc_name);
      
      #prcBody#
      
      aux_log.log (''Finish'',l_vc_prc_name);
      RETURN #returnVar#>;
   EXCEPTION
      WHEN OTHERS THEN
         aux_log.log (SQLERRM, l_vc_prc_name, aux_log.gc_error);
         #exceptionHandling#
         RAISE;
   END #prcName#;';

  /**
  * Substitute a parameter (#parameter_name#) with a text
  *
  * @param p_vc_code_string     Parameterized string
  * @param p_vc_param_name      Name of the parameter, surrounded by "#"
  * @param p_vc_param_value     Substitute text
  */
  PROCEDURE prc_set_text_param(p_vc_code_string IN OUT CLOB
                              ,p_vc_param_name  IN aux_type.vc_obj_plsql
                              ,p_vc_param_value IN CLOB);

  /**
  * Substitute standard source parameters #owner# and #dblink# with the content
  * of the global variables g_vc_src_obj_owner and g_vc_src_obj_dblink
  *
  * @param p_vc_code_string     Parameterized string
  */
  PROCEDURE prc_set_src_param(p_vc_code_string IN OUT CLOB);

  /**
  * Import metadata for table and table columns
  *
  * @param p_vc_dblink            object db link
  * @param p_vc_owner             object owner
  * @param p_vc_object_name       object name
  * @param p_vc_target_object     target object for storing metadata
  * @param p_vc_target_columns    target columns for storing metadata
  */
  PROCEDURE prc_import_metadata(p_vc_dblink            VARCHAR2
                               ,p_vc_owner             VARCHAR2
                               ,p_vc_object_name       VARCHAR2
                               ,p_vc_target_object     VARCHAR2
                               ,p_vc_target_columns    VARCHAR2 DEFAULT NULL
                               ,p_b_check_dependencies BOOLEAN DEFAULT TRUE);

  /**
  * Build a list of columns belonging to a given object
  *
  * @param p_vc_dblink            object db link
  * @param p_vc_owner             object owner
  * @param p_vc_object_name       object name
  *
  * @return table comment
  */
  FUNCTION fct_get_table_comment(p_vc_dblink      VARCHAR2
                                ,p_vc_owner       VARCHAR2
                                ,p_vc_object_name VARCHAR2) RETURN VARCHAR2;

  /**
  * Build a list of columns belonging to a given object
  *
  * @param p_vc_dblink            object db link
  * @param p_vc_owner             object owner
  * @param p_vc_object_name       object name
  * @param p_vc_column_type       Type of the column to list (PK, non-PK, ALL)
  * @param p_vc_list_type         Type of list (comma separated, assignment, use of alias)
  * @param p_vc_alias1            First alias
  * @param p_vc_alias2            Second alias
  * @param p_vc_exclude_list      List of colums to exclude
  *
  * @return List of columns
  */
  FUNCTION fct_get_column_list(p_vc_dblink       VARCHAR2
                              ,p_vc_owner        VARCHAR2
                              ,p_vc_object_name  VARCHAR2
                              ,p_vc_column_type  VARCHAR2
                              ,p_vc_list_type    VARCHAR2
                              ,p_vc_alias1       VARCHAR2 DEFAULT NULL
                              ,p_vc_alias2       VARCHAR2 DEFAULT NULL
                              ,p_vc_exclude_list VARCHAR2 DEFAULT NULL)
    RETURN VARCHAR2;

  /**
  * Build a list of columns belonging to a combination of 2 given objects
  * For example, columns in common between the 2 objects
  *
  * @param p_vc_dblink1           object 1 db link
  * @param p_vc_owner1            object 1 owner
  * @param p_vc_object1_name      object 1 name
  * @param p_vc_owner2            object 2 owner
  * @param p_vc_object2_name      object 2 name
  * @param p_vc_column_type       Type of the column to list (Common PK, Common non-PK, ALL)
  * @param p_vc_list_type         Type of list (comma separated, assignment, use of alias)
  * @param p_vc_alias1            First alias
  * @param p_vc_alias2            Second alias
  * @param p_vc_exclude_list      List of colums to exclude
  *
  * @return List of columns
  */
  FUNCTION fct_get_column_subset(p_vc_dblink1      VARCHAR2
                                ,p_vc_owner1       VARCHAR2
                                ,p_vc_object1_name VARCHAR2
                                ,p_vc_owner2       VARCHAR2
                                ,p_vc_object2_name VARCHAR2
                                ,p_vc_column_type  VARCHAR2
                                ,p_vc_list_type    VARCHAR2
                                ,p_vc_alias1       VARCHAR2 DEFAULT NULL
                                ,p_vc_alias2       VARCHAR2 DEFAULT NULL
                                ,p_vc_exclude_list VARCHAR2 DEFAULT NULL)
    RETURN VARCHAR2;

  /**
  * Execute a statement contained in a CLOB string
  *
  * @param p_sql_code      String containing the statement
  */
  PROCEDURE prc_execute(p_sql_code CLOB);

  /**
  * Copy the content of a given source table to a target table.
  * Only common columns are considered
  *
  * @param p_vc_table_name_trg    Target table
  * @param p_vc_table_name_src    Source table
  */
  PROCEDURE prc_migrate_table(p_vc_table_name_trg VARCHAR2
                             ,p_vc_table_name_src VARCHAR2);

  /**
  * Backup the content of a given source table
  * to a newly created target table with the same structure
  * Only common columns are considered
  *
  * @param p_vc_table_name    Source table
  * @param p_vc_backup_name   Backup table
  * @param p_b_raise_flag     Raise exception if backup table already exists
  */
  PROCEDURE prc_backup_table(p_vc_table_name  VARCHAR2
                            ,p_vc_backup_name VARCHAR2
                            ,p_b_raise_flag   BOOLEAN DEFAULT FALSE);

  /**
  * Drop an object
  *
  * @param p_vc_object_type   Object type
  * @param p_vc_object_name   Object name
  * @param p_b_raise_flag     Raise exception if object don't exists
  */
  PROCEDURE prc_drop_object(p_vc_object_type VARCHAR2
                           ,p_vc_object_name VARCHAR2
                           ,p_b_raise_flag   BOOLEAN DEFAULT FALSE);

  /**
  * Create a synonym for a given object
  *
  * @param p_vc_object_name   Object name
  * @param p_vc_synonym_name  Synonym name
  * @param p_b_public         Crete public synonym if true
  * @param p_b_drop_flag      Drop existing synonym if true
  * @param p_b_raise_flag     Raise exception if error occurs
  */
  PROCEDURE prc_create_synonym(p_vc_object_name  VARCHAR2
                              ,p_vc_synonym_name VARCHAR2
                              ,p_b_public        BOOLEAN DEFAULT FALSE
                              ,p_b_drop_flag     BOOLEAN DEFAULT FALSE
                              ,p_b_raise_flag    BOOLEAN DEFAULT FALSE);

  /**
  * Create an object
  *
  * @param p_vc_object_type   Object type
  * @param p_vc_object_name   Object name
  * @param p_vc_object_ddl    Object creation ddl
  * @param p_b_drop_flag      Drop existing synonym if true
  * @param p_b_raise_flag     Raise exception if error occurs
  */
  PROCEDURE prc_create_object(p_vc_object_type VARCHAR2
                             ,p_vc_object_name VARCHAR2
                             ,p_vc_object_ddl  CLOB
                             ,p_b_drop_flag    BOOLEAN DEFAULT FALSE
                             ,p_b_raise_flag   BOOLEAN DEFAULT FALSE);

  /**
  * Create entity related objects
  *
  * @param p_vc_entity_name       Entity name
  * @param p_vc_entity_fields     Entity column definitions
  * @param p_vc_create_mode       Entity creation mode ('DROP','DEFAULT')
  * @param p_b_public_flag        Create public synonym if true
  * @param p_b_migrate_flag       Migrate content in the newly created table
  * @param p_b_cdc_flag           Create cdc table and triggers
  */
  PROCEDURE prc_create_entity(p_vc_entity_name    VARCHAR2
                             ,p_vc_entity_fields  VARCHAR2
                             ,p_vc_create_mode    VARCHAR2 DEFAULT 'DEFAULT'
                             ,p_b_public_flag     BOOLEAN DEFAULT FALSE
                             ,p_b_migrate_flag    BOOLEAN DEFAULT FALSE
                             ,p_b_cdc_flag        BOOLEAN DEFAULT FALSE);

  /**
  * check if a table is partitioned
  *
  * @param p_vc_dblink       Db link for object
  * @param p_vc_owner        Owner of object
  * @param p_vc_object_name  Name of object
  */
  FUNCTION fct_check_part(p_vc_dblink      VARCHAR2
                         ,p_vc_owner       VARCHAR2
                         ,p_vc_object_name VARCHAR2) RETURN BOOLEAN;

  /**
  * check if 2 objects have the same columns
  *
  * @param p_vc_dblink1       Db link for object 1
  * @param p_vc_owner1        Owner of object 1
  * @param p_vc_object1_name  Name of object 1
  * @param p_vc_owner2        Owner of object 2
  * @param p_vc_object2_name  Name of object 2
  */
  FUNCTION fct_check_col(p_vc_dblink1      VARCHAR2
                        ,p_vc_owner1       VARCHAR2
                        ,p_vc_object1_name VARCHAR2
                        ,p_vc_owner2       VARCHAR2
                        ,p_vc_object2_name VARCHAR2) RETURN BOOLEAN;

  /**
  * check if 2 objects have the same pk-columns
  *
  * @param p_vc_dblink1       Db link for object 1
  * @param p_vc_owner1        Owner of object 1
  * @param p_vc_object1_name  Name of object 1
  * @param p_vc_owner2        Owner of object 2
  * @param p_vc_object2_name  Name of object 2
  */
  FUNCTION fct_check_pk(p_vc_dblink1      VARCHAR2
                       ,p_vc_owner1       VARCHAR2
                       ,p_vc_object1_name VARCHAR2
                       ,p_vc_owner2       VARCHAR2
                       ,p_vc_object2_name VARCHAR2) RETURN BOOLEAN;
END aux_ddl;
/
