-- SUGARCRM
DROP DATABASE sugarcrm;

DROP LOGIN sugarcrm;

COMMIT;

CREATE DATABASE sugarcrm;

CREATE LOGIN sugarcrm WITH PASSWORD='sugarcrm', DEFAULT_DATABASE=sugarcrm, CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;

ALTER AUTHORIZATION ON DATABASE::sugarcrm TO sugarcrm;

COMMIT;

-- DWH-STAGE
DROP DATABASE dwh;

DROP LOGIN dwh;

COMMIT;

CREATE DATABASE dwh;

CREATE LOGIN dwh WITH PASSWORD='dwh', DEFAULT_DATABASE=dwh, CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;

ALTER AUTHORIZATION ON DATABASE::dwh TO dwh;

use dwh;

create schema stage;

COMMIT;
