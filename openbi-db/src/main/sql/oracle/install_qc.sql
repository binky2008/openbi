/**
 * $Author: nmarangoni $
 * $Date: 2011-11-11 16:22:42 +0100 (Fr, 11 Nov 2011) $
 * $Revision: 1709 $
 * $Id: install_qc.sql 1709 2011-11-11 15:22:42Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_qc.sql $
 */


-- Tables
@tables\qc_case_t.sql;
@tables\qc_step_t.sql;
@tables\qc_keyfigure_t.sql;
@tables\qc_threshold_t.sql;
@tables\qc_exec_t.sql;

-- Views
@views\qc_meta_v.sql;
@views\qc_exec_v.sql;
@views\qc_core_propagation_v.sql;
@views\qc_stage_operation_v.sql;
@views\qc_stage_reference_v.sql;

-- Packages
@packages\pkg_qc\pkg_qc-def.sql;
@packages\pkg_qc\pkg_qc-impl.sql;
@packages\pkg_qc_core\pkg_qc_core-def.sql;
@packages\pkg_qc_core\pkg_qc_core-impl.sql;
@packages\pkg_qc_stage\pkg_qc_stage-def.sql;
@packages\pkg_qc_stage\pkg_qc_stage-impl.sql;

-- Grants
@grants\enable.sql;