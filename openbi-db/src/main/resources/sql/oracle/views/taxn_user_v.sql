CREATE OR REPLACE VIEW taxn_user_v
AS
   SELECT ut.taxn_user_id
        , ut.user_id
        , ut.taxn_id
        , us.user_code
        , us.user_email
        , ta.taxn_code
     from taxn_user_t ut
        , user_t us
        , taxn_t ta
    WHERE ut.user_id = us.user_id
      AND ut.taxn_id = ta.taxn_id;

COMMENT ON TABLE taxn_user_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON taxn_user_v TO PUBLIC;