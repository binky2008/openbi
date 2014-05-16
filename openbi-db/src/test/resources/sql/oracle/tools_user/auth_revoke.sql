BEGIN
   p#frm#auth.prc_revoke_tool ('DWHSTAGE');
   p#frm#auth.prc_revoke_trac ('DWHSTAGE');
   p#frm#auth.prc_revoke_mesr ('DWHSTAGE');
   p#frm#auth.prc_revoke_stag ('DWHSTAGE');
END;
/