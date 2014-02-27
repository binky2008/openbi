CREATE OR REPLACE PACKAGE BODY aux_doc IS
  PROCEDURE prc_set_text_param(p_vc_code_string IN OUT CLOB
                              ,p_vc_param_name  IN aux_type.vc_obj_plsql
                              ,p_vc_param_value IN CLOB) IS
    l_vc_prc_name      aux_type.vc_obj_plsql := 'PRC_SET_CODE_PARAM';
    l_vc_buffer_in     CLOB;
    l_vc_buffer_out    CLOB;
    l_vc_token         CLOB;
    l_i_position_begin NUMBER;
    l_i_position_end   NUMBER;
  BEGIN
    l_vc_buffer_in     := p_vc_code_string;
    l_i_position_begin := instr(l_vc_buffer_in
                               ,'<' || p_vc_param_name || ' />') - 1;
    l_i_position_end   := instr(l_vc_buffer_in
                               ,'<' || p_vc_param_name || ' />') +
                          length(p_vc_param_name) + 4;
  
    -- Loop on occurencies of the parameter into the root code
    WHILE l_i_position_begin > 0 LOOP
      l_vc_token         := substr(l_vc_buffer_in
                                  ,1
                                  ,l_i_position_begin);
      l_vc_buffer_out    := l_vc_buffer_out || l_vc_token;
      l_vc_buffer_out    := l_vc_buffer_out || p_vc_param_value;
      l_vc_buffer_in     := substr(l_vc_buffer_in
                                  ,l_i_position_end);
      l_i_position_begin := instr(l_vc_buffer_in
                                 ,'<' || p_vc_param_name || ' />') - 1;
      l_i_position_end   := instr(l_vc_buffer_in
                                 ,'<' || p_vc_param_name || ' />') +
                            length(p_vc_param_name) + 4;
    END LOOP;
  
    -- Append the rest token
    l_vc_buffer_out  := l_vc_buffer_out || l_vc_buffer_in;
    p_vc_code_string := l_vc_buffer_out;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END prc_set_text_param;

  FUNCTION fct_get_stylesheet(p_vc_stylesheet_type VARCHAR2) RETURN CLOB IS
  BEGIN
    IF p_vc_stylesheet_type = 'HTML' THEN
      RETURN aux_doc_template.c_xsl_html_table_default;
    ELSIF p_vc_stylesheet_type = 'HTML' THEN
      RETURN aux_doc_template.c_xsl_excel_table_default;
    ELSE
      RETURN NULL;
    END IF;
  END fct_get_stylesheet;

  FUNCTION fct_get_meta_item(p_vc_content VARCHAR2) RETURN CLOB IS
  BEGIN
    RETURN '<column-definition><column-label>' || p_vc_content || '</column-label></column-definition>';
  END fct_get_meta_item;

  FUNCTION fct_get_meta(p_vc_content CLOB) RETURN CLOB IS
  BEGIN
    RETURN '<metadata>' || p_vc_content || '</metadata>';
  END fct_get_meta;

  FUNCTION fct_get_data_cell(p_vc_content VARCHAR2) RETURN CLOB IS
  BEGIN
    RETURN '<columnValue>' || p_vc_content || '</columnValue>';
  END fct_get_data_cell;

  /**
  * Generate a record
  */
  FUNCTION fct_get_data_record(p_vc_content CLOB) RETURN CLOB IS
  BEGIN
    RETURN '<currentRow>' || p_vc_content || '</currentRow>';
  END fct_get_data_record;

  FUNCTION fct_get_data(p_vc_content CLOB) RETURN CLOB IS
  BEGIN
    RETURN '<data>' || p_vc_content || '</data>';
  END fct_get_data;

  FUNCTION fct_get_data(p_l_content aux_type.l_line_array) RETURN CLOB IS
    l_vc_content CLOB;
  BEGIN
    IF p_l_content.first IS NOT NULL THEN
      FOR i IN p_l_content.first .. p_l_content.last LOOP
        l_vc_content := l_vc_content || p_l_content(i);
      END LOOP;
    END IF;
  
    RETURN '<data>' || l_vc_content || '</data>';
  END fct_get_data;

  FUNCTION fct_get_dataset(p_vc_content CLOB) RETURN CLOB IS
  BEGIN
    RETURN '<?xml version="1.0" ?>' || '<webRowSet xmlns="http://java.sun.com/xml/ns/jdbc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://java.sun.com/xml/ns/jdbc http://java.sun.com/xml/ns/jdbc/webrowset.xsd">' || p_vc_content || '</webRowSet>';
  END fct_get_dataset;

  FUNCTION fct_get_dataset_formatted(p_vc_dataset    CLOB
                                    ,p_vc_stylesheet CLOB) RETURN CLOB IS
    l_vc_document CLOB;
  BEGIN
    SELECT xmltransform(xmltype(p_vc_dataset), xmltype(p_vc_stylesheet))
           .getclobval()
      INTO l_vc_document
      FROM dual;
  
    l_vc_document := REPLACE(l_vc_document
                            ,'><'
                            ,'>' || chr(10) || '<');
    RETURN l_vc_document;
  END fct_get_dataset_formatted;

  FUNCTION fct_get_document(p_vc_content CLOB
                           ,p_vc_type    CLOB) RETURN CLOB IS
    l_clob_document CLOB;
  BEGIN
    CASE p_vc_type
      WHEN 'html' THEN
        l_clob_document := aux_doc_template.c_html_template_content;
        prc_set_text_param(l_clob_document
                          ,'htmlScript'
                          ,aux_doc_template.c_js_default);
        prc_set_text_param(l_clob_document
                          ,'htmlStyle'
                          ,aux_doc_template.c_css_default);
        prc_set_text_param(l_clob_document
                          ,'htmlContent'
                          ,p_vc_content);
      WHEN 'ms-excel' THEN
        l_clob_document := aux_doc_template.c_excel_template_content;
        prc_set_text_param(l_clob_document
                          ,'workbookContent'
                          ,p_vc_content);
    END CASE;
  
    RETURN l_clob_document;
  END fct_get_document;

  FUNCTION fct_get_table_dataset(p_vc_table_owner  IN VARCHAR2
                                ,p_vc_table_name   IN VARCHAR2
                                ,p_vc_column_list  IN VARCHAR2 DEFAULT NULL
                                ,p_vc_where_clause IN VARCHAR2 DEFAULT NULL
                                ,p_vc_order_clause IN VARCHAR2 DEFAULT NULL)
    RETURN CLOB IS
    l_l_columns      dbms_sql.varchar2s;
    l_l_records      aux_type.l_line_array;
    l_vc_column_list VARCHAR2(32000);
    l_vc_sql         VARCHAR2(32000);
    l_xml_meta       CLOB;
    l_xml_data       CLOB;
    l_clob_report    CLOB;
  BEGIN
    -- Generate the metadata section of the webrowset
    IF p_vc_column_list IS NULL THEN
      SELECT fct_get_meta_item(column_name) BULK COLLECT
        INTO l_l_columns
        FROM all_tab_columns
       WHERE owner = upper(p_vc_table_owner)
         AND table_name = upper(p_vc_table_name)
       ORDER BY column_id;
    
      l_vc_column_list := aux_type.fct_list_to_string(l_l_columns);
    ELSE
      l_l_columns := aux_type.fct_string_to_list(p_vc_column_list
                                                ,',');
    
      FOR i IN l_l_columns.first .. l_l_columns.last LOOP
        l_vc_column_list := l_vc_column_list ||
                            fct_get_meta_item(l_l_columns(i));
      END LOOP;
    END IF;
  
    l_xml_meta := fct_get_meta(l_vc_column_list);
    -- Generate the data section of the webrowset
    l_vc_column_list := NULL;
  
    IF p_vc_column_list IS NULL THEN
      SELECT '|| aux_doc.fct_get_data_cell ("' || column_name || '")' BULK COLLECT
        INTO l_l_columns
        FROM all_tab_columns
       WHERE owner = upper(p_vc_table_owner)
         AND table_name = upper(p_vc_table_name)
       ORDER BY column_id;
    
      l_vc_column_list := ltrim(aux_type.fct_list_to_string(l_l_columns)
                               ,' ||');
    ELSE
      l_l_columns := aux_type.fct_string_to_list(p_vc_column_list
                                                ,',');
    
      FOR i IN l_l_columns.first .. l_l_columns.last LOOP
        l_vc_column_list := l_vc_column_list || CASE
                              WHEN i > 1 THEN
                               ' || '
                            END || 'aux_doc.fct_get_data_cell (' ||
                            l_l_columns(i) || ')';
      END LOOP;
    END IF;
  
    l_vc_sql := 'SELECT aux_doc.fct_get_data_record (' || l_vc_column_list ||
                ') FROM ' || p_vc_table_name || CASE
                  WHEN p_vc_where_clause IS NULL THEN
                   NULL
                  ELSE
                   ' WHERE ' || p_vc_where_clause
                END || CASE
                  WHEN p_vc_order_clause IS NULL THEN
                   NULL
                  ELSE
                   ' ORDER BY ' || p_vc_order_clause
                END;
  
    EXECUTE IMMEDIATE l_vc_sql BULK COLLECT
      INTO l_l_records;
  
    l_xml_data := fct_get_data(l_l_records);
    -- Return the complete dataset in from of a webrowset
    RETURN fct_get_dataset(l_xml_meta || l_xml_data);
  END fct_get_table_dataset;

  PROCEDURE prc_save_document(p_vc_doc_code    IN VARCHAR2
                             ,p_vc_doc_type    IN VARCHAR2
                             ,p_vc_doc_content IN CLOB
                             ,p_vc_doc_url     IN VARCHAR2 DEFAULT NULL
                             ,p_vc_doc_desc    IN VARCHAR2 DEFAULT NULL) IS
  BEGIN
    DELETE aux_doc_t
     WHERE doc_code = p_vc_doc_code
       AND doc_type = p_vc_doc_type;
  
    INSERT INTO aux_doc_t
      (doc_code
      ,doc_type
      ,doc_content
      ,doc_url
      ,doc_desc)
    VALUES
      (p_vc_doc_code
      ,p_vc_doc_type
      ,p_vc_doc_content
      ,p_vc_doc_url
      ,p_vc_doc_desc);
  
    COMMIT;
  END prc_save_document;
  /**
  * Package initialization
  */
BEGIN
  c_body_version := '$Id: $';
  c_body_url     := '$HeadURL: $';
END aux_doc;
/