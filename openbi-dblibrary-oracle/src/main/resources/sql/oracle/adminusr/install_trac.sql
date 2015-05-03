/**
 * $Author: admin $
 * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
 * $Revision: 15 $
 * $Id: install_trac.sql 15 2015-05-03 16:17:11Z admin $
 * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/install_trac.sql $
 */

@tables\trac_t.sql;

-- Views
@views\trac_v.sql;

-- Packages
@packages\trac_param\trac_param-def.sql;
@packages\trac\trac-def.sql;
@packages\trac\trac-impl.sql;
