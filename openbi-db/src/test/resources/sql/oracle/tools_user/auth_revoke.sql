BEGIN
   auth.prc_revoke_tool ('DWHSTAGE');
   auth.prc_revoke_trac ('DWHSTAGE');
   auth.prc_revoke_mesr ('DWHSTAGE');
   auth.prc_revoke_stag ('DWHSTAGE');
END;
/