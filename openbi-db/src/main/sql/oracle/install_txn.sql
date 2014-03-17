/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL:$
 */

@tables\user_t.sql;
@tables\txn_t.sql;
@tables\txn_user_t.sql;

-- Views
@views\txn_v.sql;
@views\txn_user_v.sql;

-- Packages
@packages\txn\txn-def.sql;
@packages\txn\txn-def.sql;

-- Grants
--@grants\enable.sql;