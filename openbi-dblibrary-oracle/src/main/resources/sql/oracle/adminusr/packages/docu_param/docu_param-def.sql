CREATE OR REPLACE PACKAGE p#frm#docu_param
   AUTHID CURRENT_USER
AS
   /**
   * Package containing general purpose functions and procedures
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: docu_param-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/docu_param/docu_param-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version     CONSTANT VARCHAR2 (1024) := '$Id: docu_param-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url         CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/docu_param/docu_param-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version              VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                  VARCHAR2 (1024);

   /**
   * String type
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   /**
   * Javascript for dynamic effects
   */
   c_js_default                t_string
                                  :=    '<script type="text/javascript">
function changeRowDisplay(nodeIdPath) {

  nLevelParent = nodeIdPath.split("_").length;
  arrRows = document.getElementsByTagName("tr");
  for ( i = 0; i < arrRows.length; i++ ) {
    nLevelNode = arrRows[i].id.split("_").length;
    if (arrRows[i].id.substring(0,nodeIdPath.length) == nodeIdPath) {
      if (arrRows[i].style.display=="none" '
                                     || CHR (38)
                                     || CHR (38)
                                     || ' nLevelNode == nLevelParent + 1){
        arrRows[i].style.display="block";
        document.getElementById(nodeIdPath+ "_control").innerHTML = "-";
      }
      else if (arrRows[i].style.display=="block" '
                                     || CHR (38)
                                     || CHR (38)
                                     || ' nLevelNode > nLevelParent){
        arrRows[i].style.display="none";
        document.getElementById(nodeIdPath+ "_control").innerHTML = "+";
        if (document.getElementById(arrRows[i].id + "_control")) {
          document.getElementById(arrRows[i].id + "_control").innerHTML = "+";
        }
      }
    }
  }
}
</script>';
   /**
    * CSS
    */
   c_css_default               t_string := '<style>
body {
  background-color: rgb(239, 239, 239);
}
table {
  border-style: solid;
  border-left-width: 1px;
  border-top-width: 1px;
  border-right-width: 0px;
  border-bottom-width: 0px;
  border-color: rgb(8, 36, 107);
}
th {
  border-style: solid;
  border-left-width: 0px;
  border-top-width: 0px;
  border-right-width: 1px;
  border-bottom-width: 1px;
  background-color: rgb(243, 184, 123);
  text-align: left;
}
td {
  border-style: solid;
  border-left-width: 0px;
  border-top-width: 0px;
  border-right-width: 1px;
  border-bottom-width: 1px;
  background-color: rgb(214, 223, 247);
  text-align: justify;
}
h1 {
  font-family: arial;
  color: rgb(0, 0, 102);
  margin: 0px;
}
h2 {
  font-family: arial;
  color: rgb(0, 0, 102);
  margin: 0px;
}
h3 {
  font-family: arial;
  color: rgb(0, 0, 102);
  margin: 0px;
}
p {
  font-family: arial;
  color: rgb(0, 0, 102);
  margin: 0px;
}
p.number {
  text-align: right;
}
a.control {
  background-color: rgb(255, 255, 255);
  border-style: solid;
  border-width: 1px;
  border-color: rgb(8, 36, 107);
  text-align: center;
  width: 10px;
}
</style>';
   /**
    * HTML Email Template
    */
   c_html_template_content     CLOB := '<html>
<head>
<title>Oracle Email</title>
<htmlScript />
<htmlStyle />
</head>
<body>
<htmlContent />
</body>
</html>';
   /**
    * Excel Attachment Template
    */
   c_excel_template_content    CLOB := '<?xml version="1.0"?>
<Workbook
	xmlns="urn:schemas-microsoft-com:office:spreadsheet"
	xmlns:o="urn:schemas-microsoft-com:office:office"
	xmlns:x="urn:schemas-microsoft-com:office:excel"
	xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
	xmlns:html="http://www.w3.org/TR/REC-html40"
>
<workbookContent />
</Workbook>';
   /**
    * HTML Table stylesheet
    */
   c_xsl_html_table_default    t_string := '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:data="http://java.sun.com/xml/ns/jdbc">
<xsl:param name="sort_column" />
<xsl:param name="sort_order" />
<xsl:template match="data:webRowSet">
    <table cellspacing="0" cellpadding="2">
    <tr>
    <xsl:for-each select="data:metadata/data:column-definition">
        <th>
        <xsl:attribute name="id">column<xsl:value-of select="position()" /></xsl:attribute>
        <p><xsl:value-of select="data:column-label" /></p>
        </th>
    </xsl:for-each>
    </tr>
    <xsl:for-each select="data:data/data:currentRow">
        <tr>
        <xsl:for-each select="data:columnValue">
            <td>
            <xsl:choose>
                <xsl:when test="string-length()=0">
                    <p>-</p>
                </xsl:when>
                <xsl:otherwise>
                    <p>
    			    <xsl:variable name="cell_value"><xsl:value-of select="." /></xsl:variable>
        			<xsl:choose>
        			  <xsl:when test="number($cell_value)">
        			    <xsl:attribute name="class">number</xsl:attribute>
      				  </xsl:when>
        			</xsl:choose>
                    <xsl:value-of select="." />
                    </p>
                </xsl:otherwise>
            </xsl:choose>                    
            </td>
        </xsl:for-each>
        </tr>
    </xsl:for-each>
    </table>
</xsl:template>
</xsl:stylesheet>';
   /**
    * Excel Table stylesheet
    */
   c_xsl_excel_table_default   t_string := '<xsl:stylesheet version="1.0"
	xmlns="urn:schemas-microsoft-com:office:spreadsheet"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
	xmlns:msxsl="urn:schemas-microsoft-com:xslt"
	xmlns:user="urn:my-scripts"
	xmlns:o="urn:schemas-microsoft-com:office:office"
	xmlns:x="urn:schemas-microsoft-com:office:excel"
	xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
	xmlns:data="http://java.sun.com/xml/ns/jdbc"
>
<xsl:template match="data:webRowSet">
	<Worksheet>
        <xsl:attribute name="ss:Name"><worksheetName /></xsl:attribute>
    <Table>
	<Row>
	<xsl:for-each select="data:metadata/data:column-definition">
		<Cell><Data ss:Type="String"><xsl:value-of select="data:column-label" /></Data></Cell>
	</xsl:for-each>
	</Row>
	<xsl:for-each select="data:data/data:currentRow">
		<Row>
		<xsl:for-each select="data:columnValue">
            <Cell>
			<Data>
    		<xsl:variable name="cell_value"><xsl:value-of select="." /></xsl:variable>
        		<xsl:choose>
        		    <xsl:when test="number($cell_value)">
        		        <xsl:attribute name="ss:Type">Number</xsl:attribute>
      			    </xsl:when>
                    <xsl:otherwise>
        		        <xsl:attribute name="ss:Type">String</xsl:attribute>
                    </xsl:otherwise>
        		</xsl:choose>
            <xsl:value-of select="." />
            </Data>
            </Cell>
		</xsl:for-each>
		</Row>
	</xsl:for-each>
	</Table>
	</Worksheet>
</xsl:template>
</xsl:stylesheet>';
END p#frm#docu_param;