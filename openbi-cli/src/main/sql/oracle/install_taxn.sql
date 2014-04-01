/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL:$
 */

@tables\user_t.sql;
@tables\taxn_t.sql;
@tables\taxn_user_t.sql;

-- Views
@views\taxn_v.sql;
@views\taxn_user_v.sql;

-- Packages
@packages\taxn\taxn-def.sql;
@packages\taxn\taxn-def.sql;

-- Grants
--@grants\enable.sql;