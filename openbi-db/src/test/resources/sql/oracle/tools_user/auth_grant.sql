BEGIN
   p#frm#auth.prc_grant_tool ('DWHSTAGE');
   p#frm#auth.prc_grant_trac ('DWHSTAGE');
   p#frm#auth.prc_grant_mesr ('DWHSTAGE');
   p#frm#auth.prc_grant_stag ('DWHSTAGE');
END;
/