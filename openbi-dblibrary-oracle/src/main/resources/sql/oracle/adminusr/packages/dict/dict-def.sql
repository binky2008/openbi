CREATE OR REPLACE PACKAGE p#frm#dict
   AUTHID CURRENT_USER
AS
   /**
   * Templates for standard ddls
   * APIs to construct list of columns and column definitions
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: dict-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/dict/dict-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: dict-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/dict/dict-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   /**
   * String type
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   g_vc_src_obj_owner        t_object_name;
   g_vc_src_obj_dblink       t_object_name;
   /**
   * Generic metadata retrieval statements
   */
   -- PL/SQL block to store metadata in a tmp table.
   c_sql_import_metadata     t_string := 'BEGIN
		DELETE #targetObject#;
			
		INSERT INTO #targetObject# #targetColumns#
					#sourceSelect#;
		COMMIT;
	END;';
   c_sql_tab_part            t_string := 'SELECT COUNT (*)
  FROM all_tab_partitions#dblink#
 WHERE table_owner = :ow
   AND table_name = :tb';
   -- Code token to retrieve all columns of an object and their position inside the pk.
   -- If the object is a view, try to detect PK information from an underlying table.
   -- Works for both remote and local tables.
   c_sql_obj_col_all         t_string := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:p))
     AND table_name = TRIM(UPPER(:p))';
   -- Code token to retrieve pk columns of an object and their position inside the pk.
   -- If the object is a view, try to detect PK information from an underlying table.
   -- Works for both remote and local tables.
   c_sql_obj_col_pk          t_string := 'SELECT tb.object_owner
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
   c_sql_obj_col_pk_nodep    t_string := 'SELECT co.owner AS object_owner
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
   c_sql_col_def             t_string := 'SELECT tc.column_id
	   , tc.column_name
	   , cm.comments
       , tc.data_type
       , tc.data_length
       , tc.data_precision
       , tc.data_scale
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
   c_sql_col_all             t_string := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:ow))
     AND table_name = TRIM(UPPER(:tb))
ORDER BY column_id';
   -- Get all pk columns for a given obejct.
   -- If the object is a view, try to detect PK information from an underlying table.
   -- Works for both remote and local tables.
   c_sql_col_pk              t_string := 'SELECT column_name
    FROM (#sql_obj_pk#)
	WHERE object_owner = TRIM(UPPER(:ow))
     AND object_name = TRIM(UPPER(:tb))
ORDER BY position';
   -- Get all non pk columns for a given obejct.
   -- If the object is a view, try to detect PK information from an underlying table.
   -- Works for both remote and local tables.
   c_sql_col_npk             t_string := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:ow))
     AND table_name = TRIM(UPPER(:tb))
  MINUS
  SELECT column_name
    FROM (#sql_obj_pk#)
	WHERE object_owner = TRIM(UPPER(:ow))
     AND object_name = TRIM(UPPER(:tb))';
   -- Get all columns 2 given obejcts have in common.
   c_sql_col_common_all      t_string := 'SELECT column_name
    FROM all_tab_columns#dblink#
   WHERE owner = TRIM(UPPER(:p))
     AND table_name = TRIM(UPPER(:p))
  INTERSECT
  SELECT column_name
    FROM all_tab_columns
   WHERE owner = TRIM(UPPER(:ow))
     AND table_name = TRIM(UPPER(:tb))';
   -- Get all non-pk columns 2 given obejcts have in common.
   c_sql_col_common_npk      t_string := '(SELECT column_name
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
   c_sql_tab_comm            t_string := 'SELECT comments
  FROM all_tab_comments#dblink#
 WHERE owner = TRIM(UPPER(:ow))
   AND table_name = TRIM(UPPER(:tb))';

   /**
   * Substitute a parameter (#parameter_name#) with a text
   *
   * @param p_vc_code_string     Parameterized string
   * @param p_vc_param_name      Name of the parameter, surrounded by "#"
   * @param p_vc_param_value     Substitute text
   */
   PROCEDURE prc_set_text_param (
      p_vc_code_string   IN OUT CLOB
    , p_vc_param_name    IN     VARCHAR2
    , p_vc_param_value   IN     CLOB
   );

   /**
   * Substitute standard source parameters #owner# and #dblink# with the content
   * of the global variables g_vc_src_obj_owner and g_vc_src_obj_dblink
   *
   * @param p_vc_code_string     Parameterized string
   */
   PROCEDURE prc_set_src_param (p_vc_code_string IN OUT CLOB);

   /**
   * Import metadata for table and table columns
   *
   * @param p_vc_dblink            object db link
   * @param p_vc_owner             object owner
   * @param p_vc_object_name       object name
   * @param p_vc_target_object     target object for storing metadata
   * @param p_vc_target_columns    target columns for storing metadata
   */
   PROCEDURE prc_import_metadata (
      p_vc_dblink               VARCHAR2
    , p_vc_owner                VARCHAR2
    , p_vc_object_name          VARCHAR2
    , p_vc_target_object        VARCHAR2
    , p_vc_target_columns       VARCHAR2 DEFAULT NULL
    , p_b_check_dependencies    BOOLEAN DEFAULT TRUE
   );

   /**
   * Build a list of columns belonging to a given object
   *
   * @param p_vc_dblink            object db link
   * @param p_vc_owner             object owner
   * @param p_vc_object_name       object name
   *
   * @return table comment
   */
   FUNCTION fct_get_table_comment (
      p_vc_dblink         VARCHAR2
    , p_vc_owner          VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
      RETURN VARCHAR2;

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
   FUNCTION fct_get_column_list (
      p_vc_dblink          VARCHAR2
    , p_vc_owner           VARCHAR2
    , p_vc_object_name     VARCHAR2
    , p_vc_column_type     VARCHAR2
    , p_vc_list_type       VARCHAR2
    , p_vc_alias1          VARCHAR2 DEFAULT NULL
    , p_vc_alias2          VARCHAR2 DEFAULT NULL
    , p_vc_exclude_list    VARCHAR2 DEFAULT NULL
   )
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
   FUNCTION fct_get_column_subset (
      p_vc_dblink1         VARCHAR2
    , p_vc_owner1          VARCHAR2
    , p_vc_object1_name    VARCHAR2
    , p_vc_owner2          VARCHAR2
    , p_vc_object2_name    VARCHAR2
    , p_vc_column_type     VARCHAR2
    , p_vc_list_type       VARCHAR2
    , p_vc_alias1          VARCHAR2 DEFAULT NULL
    , p_vc_alias2          VARCHAR2 DEFAULT NULL
    , p_vc_exclude_list    VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2;

   /**
   * check if a table is partitioned
   *
   * @param p_vc_dblink       Db link for object
   * @param p_vc_owner        Owner of object
   * @param p_vc_object_name  Name of object
   */
   FUNCTION fct_check_part (
      p_vc_dblink         VARCHAR2
    , p_vc_owner          VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
      RETURN BOOLEAN;

   /**
   * check if 2 objects have the same columns
   *
   * @param p_vc_dblink1       Db link for object 1
   * @param p_vc_owner1        Owner of object 1
   * @param p_vc_object1_name  Name of object 1
   * @param p_vc_owner2        Owner of object 2
   * @param p_vc_object2_name  Name of object 2
   */
   FUNCTION fct_check_col (
      p_vc_dblink1         VARCHAR2
    , p_vc_owner1          VARCHAR2
    , p_vc_object1_name    VARCHAR2
    , p_vc_owner2          VARCHAR2
    , p_vc_object2_name    VARCHAR2
   )
      RETURN BOOLEAN;

   /**
   * check if 2 objects have the same pk-columns
   *
   * @param p_vc_dblink1       Db link for object 1
   * @param p_vc_owner1        Owner of object 1
   * @param p_vc_object1_name  Name of object 1
   * @param p_vc_owner2        Owner of object 2
   * @param p_vc_object2_name  Name of object 2
   */
   FUNCTION fct_check_pk (
      p_vc_dblink1         VARCHAR2
    , p_vc_owner1          VARCHAR2
    , p_vc_object1_name    VARCHAR2
    , p_vc_owner2          VARCHAR2
    , p_vc_object2_name    VARCHAR2
   )
      RETURN BOOLEAN;
END p#frm#dict;