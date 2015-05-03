/**
 * $Author: admin $
 * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
 * $Revision: 15 $
 * $Id: install_taxn.sql 15 2015-05-03 16:17:11Z admin $
 * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/install_taxn.sql $
 */

@tables\user_t.sql;
@tables\user_uk.sql;
@tables\taxn_t.sql
@tables\taxn_uk.sql;
@tables\taxn_data.sql;
@tables\taxn_user_t.sql;
@tables\taxn_user_uk.sql;

-- Views
@views\taxn_v.sql;
@views\taxn_user_v.sql;

-- Packages
@packages\taxn\taxn-def.sql;
@packages\taxn\taxn-def.sql;
