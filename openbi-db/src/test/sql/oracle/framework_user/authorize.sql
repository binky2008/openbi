/* Formatted on 21/03/2014 11:55:52 (QP5 v5.227.12220.39754) */
BEGIN
   authorize.prc_grant_UTL ('DWHSTAGE');
   --authorize.prc_revoke_UTL ('DWHSTAGE');
   authorize.prc_grant_TRC ('DWHSTAGE');
   --authorize.prc_revoke_TRC ('DWHSTAGE');
   authorize.prc_grant_mes ('DWHSTAGE');
   --authorize.prc_revoke_mes ('DWHSTAGE');
   authorize.prc_grant_stg ('DWHSTAGE');
   --authorize.prc_revoke_stg ('DWHSTAGE');
END;
/