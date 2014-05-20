BEGIN
   frm_auth.prc_revoke_tool ('DWHSTAGE_TEST');
   frm_auth.prc_revoke_trac ('DWHSTAGE_TEST');
   frm_auth.prc_revoke_mesr ('DWHSTAGE_TEST');
   frm_auth.prc_revoke_stag ('DWHSTAGE_TEST');
END;
/