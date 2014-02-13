-- User SUGARCRM
DROP SCHEMA IF EXISTS sugarcrm;

DROP ROLE IF EXISTS sugarcrm;

CREATE ROLE sugarcrm LOGIN PASSWORD 'sugarcrm' VALID UNTIL 'infinity';

CREATE SCHEMA sugarcrm AUTHORIZATION sugarcrm;

commit;


-- User DWHSTAGE
DROP SCHEMA IF EXISTS dwhstage;

DROP ROLE IF EXISTS dwhstage;

CREATE ROLE dwhstage LOGIN PASSWORD 'dwhstage' VALID UNTIL 'infinity';

CREATE SCHEMA dwhstage AUTHORIZATION dwhstage;

commit;


-- User TEST
DROP SCHEMA IF EXISTS test;

DROP ROLE IF EXISTS test;

CREATE ROLE test LOGIN PASSWORD 'test' VALID UNTIL 'infinity';

CREATE SCHEMA test AUTHORIZATION test;

commit;