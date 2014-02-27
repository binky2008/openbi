CREATE OR REPLACE PACKAGE BODY aux_ddl AS
  /**
  * $Author: nmarangoni $
  * $Date: 2012-06-08 15:34:39 +0200 (Fr, 08 Jun 2012) $
  * $Revision: 2858 $
  * $Id: $
  * $HeadURL: $
  */
  PROCEDURE prc_set_text_param(p_vc_code_string IN OUT CLOB
                              ,p_vc_param_name  IN aux_type.vc_obj_plsql
                              ,p_vc_param_value IN CLOB) IS
    l_vc_prc_name      aux_type.vc_obj_plsql := 'PRC_SET_TEXT_PARAM';
    l_vc_buffer_in     CLOB;
    l_vc_buffer_out    CLOB;
    l_vc_token         CLOB;
    l_i_position_begin INTEGER;
    l_i_position_end   INTEGER;
  BEGIN
    l_vc_buffer_in     := p_vc_code_string;
    l_i_position_begin := instr(l_vc_buffer_in
                               ,'#' || p_vc_param_name || '#') - 1;
    l_i_position_end   := instr(l_vc_buffer_in
                               ,'#' || p_vc_param_name || '#') +
                          length(p_vc_param_name) + 2;
  
    -- Loop on occurencies of the parameter into the root code
    WHILE l_i_position_begin >= 0 LOOP
      l_vc_token         := substr(l_vc_buffer_in
                                  ,1
                                  ,l_i_position_begin);
      l_vc_buffer_out    := l_vc_buffer_out || l_vc_token;
      l_vc_buffer_out    := l_vc_buffer_out || p_vc_param_value;
      l_vc_buffer_in     := substr(l_vc_buffer_in
                                  ,l_i_position_end);
      l_i_position_begin := instr(l_vc_buffer_in
                                 ,'#' || p_vc_param_name || '#') - 1;
      l_i_position_end   := instr(l_vc_buffer_in
                                 ,'#' || p_vc_param_name || '#') +
                            length(p_vc_param_name) + 2;
    END LOOP;
  
    -- Append the rest token
    l_vc_buffer_out  := l_vc_buffer_out || l_vc_buffer_in;
    p_vc_code_string := l_vc_buffer_out;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END prc_set_text_param;

  PROCEDURE prc_set_src_param(p_vc_code_string IN OUT CLOB) IS
    l_vc_prc_name aux_type.vc_max_plsql := 'PRC_SET_SRC_PARAM';
  BEGIN
    prc_set_text_param(p_vc_code_string
                      ,'owner'
                      ,CASE WHEN g_vc_src_obj_owner IS NOT NULL THEN
                       g_vc_src_obj_owner || '.' END);
    prc_set_text_param(p_vc_code_string
                      ,'dblink'
                      ,CASE WHEN g_vc_src_obj_dblink IS NOT NULL THEN
                       '@' || g_vc_src_obj_dblink END);
  END prc_set_src_param;

  PROCEDURE prc_import_metadata(p_vc_dblink            VARCHAR2
                               ,p_vc_owner             VARCHAR2
                               ,p_vc_object_name       VARCHAR2
                               ,p_vc_target_object     VARCHAR2
                               ,p_vc_target_columns    VARCHAR2 DEFAULT NULL
                               ,p_b_check_dependencies BOOLEAN DEFAULT TRUE) IS
    l_sql_col_def         aux_type.vc_max_plsql := c_sql_col_def;
    l_sql_import_metadata aux_type.vc_max_plsql := c_sql_import_metadata;
  BEGIN
    g_vc_src_obj_dblink := p_vc_dblink;
    prc_set_text_param(l_sql_col_def
                      ,'sql_obj_pk'
                      ,CASE WHEN p_b_check_dependencies THEN
                       c_sql_obj_col_pk ELSE c_sql_obj_col_pk_nodep END);
    aux_ddl.prc_set_src_param(l_sql_col_def);
    prc_set_text_param(l_sql_import_metadata
                      ,'targetObject'
                      ,p_vc_target_object);
    prc_set_text_param(l_sql_import_metadata
                      ,'targetColumns'
                      ,CASE WHEN p_vc_target_columns IS NOT NULL THEN
                       '(' || p_vc_target_columns || ')' END);
    prc_set_text_param(l_sql_import_metadata
                      ,'sourceSelect'
                      ,l_sql_col_def);
  
    EXECUTE IMMEDIATE l_sql_import_metadata
      USING p_vc_owner, p_vc_object_name;
  
    COMMIT;
  END prc_import_metadata;

  FUNCTION fct_get_table_comment(p_vc_dblink      VARCHAR2
                                ,p_vc_owner       VARCHAR2
                                ,p_vc_object_name VARCHAR2) RETURN VARCHAR2 IS
    l_sql_tab_comm aux_type.vc_max_plsql := c_sql_tab_comm;
    l_vc_tab_comm  aux_type.vc_max_plsql;
  BEGIN
    g_vc_src_obj_dblink := p_vc_dblink;
    prc_set_src_param(l_sql_tab_comm);
  
    EXECUTE IMMEDIATE l_sql_tab_comm
      INTO l_vc_tab_comm
      USING p_vc_owner, p_vc_object_name;
  
    RETURN l_vc_tab_comm;
  END fct_get_table_comment;

  FUNCTION fct_get_column_list(p_vc_dblink       VARCHAR2
                              ,p_vc_owner        VARCHAR2
                              ,p_vc_object_name  VARCHAR2
                              ,p_vc_column_type  VARCHAR2
                              ,p_vc_list_type    VARCHAR2
                              ,p_vc_alias1       VARCHAR2 DEFAULT NULL
                              ,p_vc_alias2       VARCHAR2 DEFAULT NULL
                              ,p_vc_exclude_list VARCHAR2 DEFAULT NULL)
    RETURN VARCHAR2 IS
    TYPE t_cur_ref IS REF CURSOR;
  
    l_cur_ref     t_cur_ref;
    l_sql_col_all aux_type.vc_max_plsql := c_sql_col_all;
    l_sql_col_npk aux_type.vc_max_plsql := c_sql_col_npk;
    l_sql_col_pk  aux_type.vc_max_plsql := c_sql_col_pk;
    l_vc_buffer   aux_type.vc_max_plsql;
    l_vc_list     aux_type.vc_max_plsql;
    l_vc_owner    aux_type.vc_obj_plsql;
  BEGIN
    g_vc_src_obj_dblink := p_vc_dblink;
    l_vc_owner          := nvl(p_vc_owner
                              ,sys_context('USERENV'
                                          ,'CURRENT_USER'));
  
    IF p_vc_column_type = 'ALL' THEN
      prc_set_src_param(l_sql_col_all);
    
      OPEN l_cur_ref FOR l_sql_col_all
        USING l_vc_owner, p_vc_object_name;
    ELSIF p_vc_column_type = 'PK' THEN
      prc_set_text_param(l_sql_col_pk
                        ,'sql_obj_pk'
                        ,c_sql_obj_col_pk);
      prc_set_src_param(l_sql_col_pk);
    
      OPEN l_cur_ref FOR l_sql_col_pk
        USING l_vc_owner, p_vc_object_name;
    ELSIF p_vc_column_type = 'NPK' THEN
      prc_set_text_param(l_sql_col_npk
                        ,'sql_obj_pk'
                        ,c_sql_obj_col_pk);
      prc_set_src_param(l_sql_col_npk);
    
      OPEN l_cur_ref FOR l_sql_col_npk
        USING l_vc_owner, p_vc_object_name, l_vc_owner, p_vc_object_name;
    END IF;
  
    LOOP
      FETCH l_cur_ref
        INTO l_vc_buffer;
    
      IF p_vc_exclude_list IS NULL OR
         instr(p_vc_exclude_list
              ,l_vc_buffer) = 0 THEN
        EXIT WHEN l_cur_ref%NOTFOUND;
        l_vc_list := l_vc_list || chr(10) ||
                     CASE p_vc_list_type
                       WHEN 'LIST_SIMPLE' THEN
                        l_vc_buffer || ', '
                       WHEN 'LIST_ALIAS' THEN
                        p_vc_alias1 || '.' || l_vc_buffer || ', '
                       WHEN 'SET_ALIAS' THEN
                        p_vc_alias1 || '.' || l_vc_buffer || ' = ' ||
                        p_vc_alias2 || '.' || l_vc_buffer || ', '
                       WHEN 'LIST_NVL2' THEN
                        'NVL2 (' || p_vc_alias1 || '.rowid, ' || p_vc_alias1 || '.' ||
                        l_vc_buffer || ', ' || p_vc_alias2 || '.' ||
                        l_vc_buffer || ') AS ' || l_vc_buffer || ', '
                       WHEN 'AND_NOTNULL' THEN
                        l_vc_buffer || ' IS NOT NULL AND '
                       WHEN 'AND_NULL' THEN
                        l_vc_buffer || ' IS NOT NULL AND '
                       WHEN 'AND_ALIAS' THEN
                        p_vc_alias1 || '.' || l_vc_buffer || ' = ' ||
                        p_vc_alias2 || '.' || l_vc_buffer || ' AND '
                       WHEN 'OR_DECODE' THEN
                        'DECODE (' || p_vc_alias1 || '.' || l_vc_buffer || ', ' ||
                        p_vc_alias2 || '.' || l_vc_buffer ||
                        ', 0, 1) = 1 OR '
                     END;
      END IF;
    END LOOP;
  
    CLOSE l_cur_ref;
  
    IF p_vc_list_type IN ('LIST_SIMPLE'
                         ,'LIST_ALIAS'
                         ,'LIST_NVL2'
                         ,'SET_ALIAS') THEN
      l_vc_list := rtrim(l_vc_list
                        ,', ');
    ELSIF p_vc_list_type IN ('AND_NOTNULL'
                            ,'AND_NULL'
                            ,'AND_ALIAS') THEN
      l_vc_list := substr(l_vc_list
                         ,1
                         ,length(l_vc_list) - 5);
    ELSIF p_vc_list_type = 'OR_DECODE' THEN
      l_vc_list := substr(l_vc_list
                         ,1
                         ,length(l_vc_list) - 4);
    END IF;
  
    RETURN l_vc_list;
  END fct_get_column_list;

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
    RETURN VARCHAR2 IS
    TYPE t_cur_ref IS REF CURSOR;
  
    l_cur_ref            t_cur_ref;
    l_sql_col_common_all aux_type.vc_max_plsql := c_sql_col_common_all;
    l_sql_col_common_npk aux_type.vc_max_plsql := c_sql_col_common_npk;
    l_vc_buffer          aux_type.vc_max_plsql;
    l_vc_list            aux_type.vc_max_plsql;
    l_vc_owner1          aux_type.vc_obj_plsql;
    l_vc_owner2          aux_type.vc_obj_plsql;
  BEGIN
    g_vc_src_obj_dblink := p_vc_dblink1;
    l_vc_owner1         := nvl(p_vc_owner1
                              ,sys_context('USERENV'
                                          ,'CURRENT_USER'));
    l_vc_owner2         := nvl(p_vc_owner2
                              ,sys_context('USERENV'
                                          ,'CURRENT_USER'));
  
    IF p_vc_column_type = 'COMMON_ALL' THEN
      prc_set_src_param(l_sql_col_common_all);
    
      OPEN l_cur_ref FOR l_sql_col_common_all
        USING l_vc_owner1, p_vc_object1_name, l_vc_owner2, p_vc_object2_name;
    ELSIF p_vc_column_type = 'COMMON_NPK' THEN
      prc_set_text_param(l_sql_col_common_npk
                        ,'sql_obj_pk'
                        ,c_sql_obj_col_pk);
      prc_set_src_param(l_sql_col_common_npk);
    
      OPEN l_cur_ref FOR l_sql_col_common_npk
        USING l_vc_owner1, p_vc_object1_name, l_vc_owner1, p_vc_object1_name, l_vc_owner2, p_vc_object2_name, l_vc_owner2, p_vc_object2_name, l_vc_owner2, p_vc_object2_name;
    END IF;
  
    LOOP
      FETCH l_cur_ref
        INTO l_vc_buffer;
    
      EXIT WHEN l_cur_ref%NOTFOUND;
    
      IF p_vc_exclude_list IS NULL OR
         instr(p_vc_exclude_list
              ,l_vc_buffer) = 0 THEN
        l_vc_list := l_vc_list || chr(10) ||
                     CASE p_vc_list_type
                       WHEN 'LIST_SIMPLE' THEN
                        l_vc_buffer || ', '
                       WHEN 'LIST_ALIAS' THEN
                        p_vc_alias1 || '.' || l_vc_buffer || ', '
                       WHEN 'SET_ALIAS' THEN
                        p_vc_alias1 || '.' || l_vc_buffer || ' = ' ||
                        p_vc_alias2 || '.' || l_vc_buffer || ', '
                       WHEN 'AND_ALIAS' THEN
                        p_vc_alias1 || '.' || l_vc_buffer || ' = ' ||
                        p_vc_alias2 || '.' || l_vc_buffer || ' AND '
                       WHEN 'LIST_NVL2' THEN
                        'NVL2 (' || p_vc_alias1 || '.rowid, ' || p_vc_alias1 || '.' ||
                        l_vc_buffer || ', ' || p_vc_alias2 || '.' ||
                        l_vc_buffer || ') AS ' || l_vc_buffer || ', '
                       WHEN 'OR_DECODE' THEN
                        'DECODE (' || p_vc_alias1 || '.' || l_vc_buffer || ', ' ||
                        p_vc_alias2 || '.' || l_vc_buffer ||
                        ', 0, 1) = 1 OR '
                     END;
      END IF;
    END LOOP;
  
    CLOSE l_cur_ref;
  
    IF p_vc_list_type IN ('LIST_SIMPLE'
                         ,'LIST_ALIAS'
                         ,'LIST_NVL2'
                         ,'SET_ALIAS') THEN
      l_vc_list := rtrim(l_vc_list
                        ,', ');
    ELSIF p_vc_list_type = 'AND_ALIAS' THEN
      l_vc_list := substr(l_vc_list
                         ,1
                         ,length(l_vc_list) - 5);
    ELSIF p_vc_list_type = 'OR_DECODE' THEN
      l_vc_list := substr(l_vc_list
                         ,1
                         ,length(l_vc_list) - 4);
    END IF;
  
    RETURN l_vc_list;
  END fct_get_column_subset;

  FUNCTION fct_get_table_migrate_stmt(p_vc_table_name_trg VARCHAR2
                                     ,p_vc_table_name_src VARCHAR2)
    RETURN CLOB IS
    l_vc_column_list VARCHAR2(32000);
  BEGIN
    l_vc_column_list := fct_get_column_subset(NULL
                                             ,null
                                             ,upper(p_vc_table_name_trg)
                                             ,null
                                             ,upper(p_vc_table_name_src)
                                             ,'COMMON_ALL'
                                             ,'LIST_SIMPLE');
    RETURN 'INSERT INTO ' || p_vc_table_name_trg || '(' || l_vc_column_list || ') SELECT ' || l_vc_column_list || ' FROM ' || p_vc_table_name_src;
  END fct_get_table_migrate_stmt;

  PROCEDURE prc_execute(p_sql_code CLOB) IS
    l_vcs_code    dbms_sql.varchar2s;
    l_i_cursor_id INTEGER;
  BEGIN
    l_vcs_code    := aux_type.fct_clob_to_list(p_sql_code);
    l_i_cursor_id := dbms_sql.open_cursor;
    dbms_sql.parse(l_i_cursor_id
                  ,l_vcs_code
                  ,l_vcs_code.first
                  ,l_vcs_code.last
                  ,TRUE
                  ,dbms_sql.native);
    dbms_sql.close_cursor(l_i_cursor_id);
  EXCEPTION
    when others then
      dbms_sql.close_cursor(l_i_cursor_id);
      for i in l_vcs_code.first .. l_vcs_code.last loop      
        dbms_output.put_line(l_vcs_code(i));
      end loop;
      RAISE;
  END prc_execute;

  PROCEDURE prc_migrate_table(p_vc_table_name_trg VARCHAR2
                             ,p_vc_table_name_src VARCHAR2) IS
  begin
    execute immediate fct_get_table_migrate_stmt(upper(trim(p_vc_table_name_trg))
                                                ,upper(TRIM(p_vc_table_name_src)));
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END prc_migrate_table;

  PROCEDURE prc_backup_table(p_vc_table_name  VARCHAR2
                            ,p_vc_backup_name VARCHAR2
                            ,p_b_raise_flag   BOOLEAN DEFAULT FALSE) IS
  BEGIN
    prc_drop_object('TABLE'
                   ,p_vc_backup_name);
  
    EXECUTE IMMEDIATE 'CREATE TABLE ' || p_vc_backup_name ||
                      ' AS SELECT * FROM ' || p_vc_table_name;
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END prc_backup_table;

  PROCEDURE prc_drop_object(p_vc_object_type VARCHAR2
                           ,p_vc_object_name VARCHAR2
                           ,p_b_raise_flag   BOOLEAN DEFAULT FALSE) IS
    l_vc_prc_name VARCHAR2(50) := 'PRC_DROP_OBJECT';
    l_ddl_drop    VARCHAR2(32000);
  BEGIN
    l_ddl_drop := 'DROP ' || p_vc_object_type || ' ' || p_vc_object_name;
  
    EXECUTE IMMEDIATE l_ddl_drop;
  EXCEPTION
    WHEN OTHERS THEN
      IF p_b_raise_flag THEN
        RAISE;
      END IF;
  END prc_drop_object;

  PROCEDURE prc_create_synonym(p_vc_object_name  VARCHAR2
                              ,p_vc_synonym_name VARCHAR2
                              ,p_b_public        BOOLEAN DEFAULT FALSE
                              ,p_b_drop_flag     BOOLEAN DEFAULT FALSE
                              ,p_b_raise_flag    BOOLEAN DEFAULT FALSE) IS
    l_vc_object_type aux_type.vc_obj_plsql;
  begin
    l_vc_object_type := CASE
                          WHEN p_b_public THEN
                           'PUBLIC '
                        END || 'SYNONYM';
  
    IF p_b_drop_flag THEN
      prc_drop_object(l_vc_object_type
                     ,p_vc_synonym_name
                     ,FALSE);
    END IF;
  
  
    EXECUTE IMMEDIATE 'CREATE ' || l_vc_object_type || ' ' ||
                      p_vc_synonym_name || ' FOR ' || p_vc_object_name;
  EXCEPTION
    WHEN OTHERS THEN
      IF p_b_raise_flag THEN
        RAISE;
      END IF;
  END prc_create_synonym;

  PROCEDURE prc_create_object(p_vc_object_type VARCHAR2
                             ,p_vc_object_name VARCHAR2
                             ,p_vc_object_ddl  CLOB
                             ,p_b_drop_flag    BOOLEAN DEFAULT FALSE
                             ,p_b_raise_flag   BOOLEAN DEFAULT FALSE) IS
    l_vc_object_ddl CLOB;
  BEGIN
    IF p_b_drop_flag AND
       p_vc_object_type NOT IN ('PACKAGE BODY'
                               ,'CONSTRAINT') THEN
      prc_drop_object(p_vc_object_type
                     ,p_vc_object_name
                     ,FALSE);
    END IF;
  
    l_vc_object_ddl := p_vc_object_ddl;
  
    IF p_vc_object_type IN ('PACKAGE') THEN
      aux_ddl.prc_set_text_param(l_vc_object_ddl
                                ,'generatingVersion'
                                ,aux_version.fct_get_header);
    END IF;
  
    BEGIN
      prc_execute(l_vc_object_ddl);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_b_raise_flag THEN
          RAISE;
        END IF;
    END;
  END;

  PROCEDURE prc_create_entity(p_vc_entity_name   VARCHAR2
                             ,p_vc_entity_fields VARCHAR2
                             ,p_vc_create_mode   VARCHAR2 DEFAULT 'DEFAULT'
                             ,p_b_public_flag    BOOLEAN DEFAULT FALSE
                             ,p_b_migrate_flag   BOOLEAN DEFAULT FALSE
                             ,p_b_cdc_flag       BOOLEAN DEFAULT FALSE) IS
    l_name_entity_tab VARCHAR2(100);
    l_name_entity_cdc VARCHAR2(100);
    l_name_entity_bkp VARCHAR2(100);
    l_name_entity_cbk VARCHAR2(100);
    l_name_entity_seq varchar2(100);
    l_name_entity_id VARCHAR2(100);
    l_name_entity_pk VARCHAR2(100);
    l_columns_all  CLOB;
    l_columns_old  CLOB;
    l_columns_new  CLOB;
    l_sql_create   CLOB;
    l_n_cnt_tab    NUMBER;
    l_n_cnt_hst    NUMBER;
  BEGIN
    -- Set name of physical objects
    l_name_entity_tab := c_name_entity_tab;
    prc_set_text_param(l_name_entity_tab
                      ,'entityName'
                      ,p_vc_entity_name);
    l_name_entity_cdc := c_name_entity_cdc;
    prc_set_text_param(l_name_entity_cdc
                      ,'entityName'
                      ,p_vc_entity_name);
    l_name_entity_seq := c_name_entity_seq;
    prc_set_text_param(l_name_entity_seq
                      ,'entityName'
                      ,p_vc_entity_name);
    l_name_entity_id := c_name_entity_id;
    prc_set_text_param(l_name_entity_id
                      ,'entityName'
                      ,p_vc_entity_name);
    l_name_entity_pk := c_name_entity_pk;
    prc_set_text_param(l_name_entity_pk
                      ,'entityName'
                      ,p_vc_entity_name);
  
    IF p_b_migrate_flag THEN
      SELECT COUNT(0)
        INTO l_n_cnt_tab
        FROM user_tables
       WHERE table_name = TRIM(upper(l_name_entity_tab));
    
      IF l_n_cnt_tab > 0 THEN
        l_name_entity_bkp := c_name_entity_bkp;
        prc_set_text_param(l_name_entity_bkp
                          ,'entityName'
                          ,p_vc_entity_name);
        prc_backup_table(l_name_entity_tab
                        ,l_name_entity_bkp);
      END IF;
    END IF;
  
    IF p_b_migrate_flag AND p_b_cdc_flag THEN
      SELECT COUNT(0)
        INTO l_n_cnt_hst
        FROM user_tables
       WHERE table_name = TRIM(upper(l_name_entity_cdc));
    
      IF l_n_cnt_hst > 0 THEN
        l_name_entity_cbk := c_name_entity_cbk;
        prc_set_text_param(l_name_entity_cbk
                          ,'entityName'
                          ,p_vc_entity_name);
        prc_backup_table(l_name_entity_cdc
                        ,l_name_entity_cbk);
      END IF;
    END IF;
  
    -- Drop physical objects if required
    IF p_vc_create_mode = 'DROP' THEN
      -- Drop table
      prc_drop_object('TABLE'
                     ,l_name_entity_tab);
    
      IF NOT p_b_migrate_flag THEN
        -- Drop sequence
        prc_drop_object('SEQUENCE'
                       ,l_name_entity_seq);
      END IF;
    END IF;
  
    IF p_vc_create_mode = 'DROP' AND p_b_cdc_flag THEN
      -- Drop table
      prc_drop_object('TABLE'
                     ,l_name_entity_cdc);
    END IF;
  
    -- Create table
    l_sql_create := c_template_entity_tab;
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityTable'
                              ,l_name_entity_tab);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityId'
                              ,l_name_entity_id);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityPK'
                              ,l_name_entity_pk);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'columnDefinitionList'
                              ,p_vc_entity_fields);
    prc_execute(l_sql_create);
  
    IF p_b_cdc_flag THEN
      -- Create CDC table
      l_sql_create := c_template_entity_cdc;
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'entityCDC'
                                ,l_name_entity_cdc);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'entityId'
                                ,l_name_entity_id);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'columnDefinitionList'
                                ,p_vc_entity_fields);
      prc_execute(l_sql_create);
    END IF;
  
    -- Create sequence
    l_sql_create := c_template_entity_seq;
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entitySequence'
                              ,l_name_entity_seq);
  
    BEGIN
      prc_execute(l_sql_create);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  
    -- Create triggers
    l_sql_create := c_template_entity_trg_ins;
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityName'
                              ,p_vc_entity_name);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityTable'
                              ,l_name_entity_tab);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityId'
                              ,l_name_entity_id);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entitySequence'
                              ,l_name_entity_seq);
    prc_execute(l_sql_create);
    l_sql_create := c_template_entity_trg_upd;
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityName'
                              ,p_vc_entity_name);
    aux_ddl.prc_set_text_param(l_sql_create
                              ,'entityTable'
                              ,l_name_entity_tab);
    prc_execute(l_sql_create);
    
  
    IF p_b_public_flag THEN
      prc_create_synonym(l_name_entity_tab
                        ,l_name_entity_tab
                        ,TRUE);
    
      EXECUTE IMMEDIATE 'GRANT SELECT ON ' || l_name_entity_tab ||
                        ' TO PUBLIC';
    END IF;
  
    IF p_b_migrate_flag AND l_n_cnt_tab > 0 THEN
      -- Migrate content
      prc_migrate_table(l_name_entity_tab
                       ,l_name_entity_bkp);
    END IF;
    
    IF p_b_migrate_flag AND p_b_cdc_flag AND l_n_cnt_hst > 0 THEN
      -- Migrate history content
      prc_migrate_table(l_name_entity_cdc
                       ,l_name_entity_cbk);
    end if;
    
    IF p_b_cdc_flag THEN
      -- Create CDC trigger
      l_columns_all := fct_get_column_list(NULL
                                          ,NULL
                                          ,upper(l_name_entity_tab)
                                          ,'ALL'
                                          ,'LIST_SIMPLE');
      l_columns_old := fct_get_column_list(NULL
                                          ,NULL
                                          ,upper(l_name_entity_tab)
                                          ,'ALL'
                                          ,'LIST_ALIAS'
                                          ,':OLD');
      l_columns_new := fct_get_column_list(NULL
                                          ,NULL
                                          ,upper(l_name_entity_tab)
                                          ,'ALL'
                                          ,'LIST_ALIAS'
                                          ,':NEW');
      l_sql_create  := c_template_entity_trg_cdc;
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'entityName'
                                ,p_vc_entity_name);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'entityTable'
                                ,l_name_entity_tab);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'entityCDC'
                                ,l_name_entity_cdc);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'columnList'
                                ,l_columns_all);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'columnListOld'
                                ,l_columns_old);
      aux_ddl.prc_set_text_param(l_sql_create
                                ,'columnListNew'
                                ,l_columns_new);
      prc_execute(l_sql_create);
    END IF;
  END prc_create_entity;

  FUNCTION fct_check_part(p_vc_dblink      VARCHAR2
                         ,p_vc_owner       VARCHAR2
                         ,p_vc_object_name VARCHAR2) RETURN BOOLEAN IS
    l_n_cnt_part   NUMBER;
    l_sql_tab_part aux_type.vc_max_plsql := c_sql_tab_part;
  BEGIN
    g_vc_src_obj_dblink := p_vc_dblink;
    prc_set_src_param(l_sql_tab_part);
  
    EXECUTE IMMEDIATE l_sql_tab_part
      INTO l_n_cnt_part
      USING p_vc_owner, p_vc_object_name;
  
    IF l_n_cnt_part = 0 THEN
      RETURN FALSE;
    ELSE
      RETURN TRUE;
    END IF;
  END fct_check_part;

  FUNCTION fct_check_col(p_vc_dblink1      VARCHAR2
                        ,p_vc_owner1       VARCHAR2
                        ,p_vc_object1_name VARCHAR2
                        ,p_vc_owner2       VARCHAR2
                        ,p_vc_object2_name VARCHAR2) RETURN BOOLEAN IS
    l_vc_col_all_1 aux_type.vc_max_plsql;
    l_vc_col_all_2 aux_type.vc_max_plsql;
  BEGIN
    NULL;
  END fct_check_col;

  FUNCTION fct_check_pk(p_vc_dblink1      VARCHAR2
                       ,p_vc_owner1       VARCHAR2
                       ,p_vc_object1_name VARCHAR2
                       ,p_vc_owner2       VARCHAR2
                       ,p_vc_object2_name VARCHAR2) RETURN BOOLEAN IS
    l_vc_col_pk_1 aux_type.vc_max_plsql;
    l_vc_col_pk_2 aux_type.vc_max_plsql;
  BEGIN
    l_vc_col_pk_1 := fct_get_column_list(p_vc_dblink1
                                        ,p_vc_owner1
                                        ,p_vc_object1_name
                                        ,'PK'
                                        ,'LIST_SIMPLE');
    l_vc_col_pk_2 := fct_get_column_list(NULL
                                        ,p_vc_owner2
                                        ,p_vc_object2_name
                                        ,'PK'
                                        ,'LIST_SIMPLE');
  
    IF l_vc_col_pk_1 = l_vc_col_pk_2 OR
       (l_vc_col_pk_1 IS NULL AND l_vc_col_pk_2 IS NULL) THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;
  END fct_check_pk;
BEGIN
  -- Versioning constants
  c_body_version := '$Id: $';
  c_body_url     := '$HeadURL: $';
END aux_ddl;
/
