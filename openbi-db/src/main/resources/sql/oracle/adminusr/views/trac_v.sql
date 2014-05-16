CREATE OR REPLACE VIEW trac_v
AS
     SELECT *
       FROM trac_t
   ORDER BY trac_id DESC;
