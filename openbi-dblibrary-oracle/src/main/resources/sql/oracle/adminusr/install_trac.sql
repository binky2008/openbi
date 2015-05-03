/**
 * $Author: admin $
 * $Date: 2015-05-03 17:35:13 +0200 (So, 03 Mai 2015) $
 * $Revision: 5 $
 * $Id: install_trac.sql 5 2015-05-03 15:35:13Z admin $
 * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/install_trac.sql $
 */

@tables\trac_t.sql;

-- Views
@views\trac_v.sql;

-- Packages
@packages\trac_param\trac_param-def.sql;
@packages\trac\trac-def.sql;
@packages\trac\trac-impl.sql;
