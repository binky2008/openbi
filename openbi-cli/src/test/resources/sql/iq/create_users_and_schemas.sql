-- User TEST
CREATE USER test IDENTIFIED BY 'test'
GRANT ROLE PUBLIC TO test WITH NO ADMIN OPTION;
GRANT CREATE PROCEDURE, CREATE TABLE, CREATE VIEW TO test WITH NO ADMIN OPTION;