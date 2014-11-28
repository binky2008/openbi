CREATE OR REPLACE VIEW p#frm#taxn_user_v
AS
   SELECT ut.taxn_user_id
        , ut.user_id
        , ut.taxn_id
        , us.user_code
        , us.user_email
        , ta.taxn_code
     FROM p#frm#taxn_user_t ut
        , p#frm#user_t us
        , p#frm#taxn_t ta
    WHERE ut.user_id = us.user_id
      AND ut.taxn_id = ta.taxn_id;