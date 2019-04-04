CREATE OR REPLACE PACKAGE ai_bcms_cii_returns_pkg AS
-- ***********************************************************************************
-- * Program          : ai_bcms_cii_returns_pkg specification
-- * Original Author  : Iain High
-- * System           : Animal Inspections
-- * Purpose          : Extract Inspection, Holding and Animal details from completed inspections
-- *                    in preparation for the data to be sent to BCMS.
-- *                    Created using various code from BAU AI system from the 'Saturday Return 
-- *                    Data to BCMS' Redwood Scheduler job (red_insp_bcms_extract.sql)
-- *                    and the following scripts which are called by the above job -
-- *                          bcms_cre_file.sql
-- *                          bcmsins.sql
-- *                          bcmshol.sql
-- *                          bcmsani.sql
-- *                    Some various other pieces of BAU code too.
-- *                    Used going forward to support the new LIS System.
-- *
-- * Who              Ver   When         Description
-- * ---------------  ----  -----------  ---------------------------------------------
-- * Iain High        1     09-MAY-2018  Initial Build
-- * Iain High        2     01-FEB-2019  Baseline Version to resume development in Feb 2019.
-- * Iain High        3     05-FEB-2019  Added PROCEDURE p_bcms_cii_remove_inspection.
-- *                                     Added FUNCTION fn_lead_inspector_name.
-- * Iain High        4     07-FEB-2019  Added p_user to PROCEDURE p_bcms_cii_add_inspection
-- *                                     and PROCEDURE p_bcms_cii_pop_inspections.
-- * Iain High        5     15-MAR-2019  Added PROCEDURE p_update_max_batch_size.
-- * Iain High        6     19-MAR-2019  Renamed PROCEDURE p_bcms_cii_rollback_last_batch to p_bcms_cii_rollback_batch.
-- * Branch1 changes
-- ***********************************************************************************

  FUNCTION fn_lead_inspector_name ( p_inspection_id IN inspections_all.i_id%TYPE) RETURN CHAR;

  PROCEDURE p_bcms_cii_delete_old_tmp_data     ( p_raise_error   IN  VARCHAR2
                                                ,p_rtc           OUT INTEGER);

  PROCEDURE p_bcms_cii_rollback_batch ( p_file_ref      IN  bcms_cii_results.bcr_file_ref%TYPE
                                       ,p_raise_error   IN  VARCHAR2
                                       ,p_rtc           OUT INTEGER);

  PROCEDURE p_bcms_cii_add_inspection  ( p_inspection_id    IN inspections_all.i_id%TYPE
                                        ,p_user             IN  VARCHAR2
                                        ,p_raise_error      IN  VARCHAR2
                                        ,p_rtc              OUT INTEGER);

  PROCEDURE p_bcms_cii_remove_inspection  ( p_inspection_id    IN inspections_all.i_id%TYPE
                                           ,p_raise_error      IN  VARCHAR2
                                           ,p_rtc              OUT INTEGER);

  PROCEDURE p_bcms_cii_pop_inspections ( p_inspection_count IN  INTEGER
                                        ,p_user             IN  VARCHAR2
                                        ,p_raise_error      IN  VARCHAR2
                                        ,p_rtc              OUT INTEGER);

  PROCEDURE p_bcms_cii_pop_livestock   ( p_raise_error   IN  VARCHAR2
                                        ,p_rtc           OUT INTEGER);

  PROCEDURE p_bcms_cii_return_inspections ( p_file_ref    IN  VARCHAR2
                                           ,p_raise_error   IN  VARCHAR2
                                           ,p_rtc           OUT INTEGER);
                            
  PROCEDURE p_bcms_cii_return_holdings ( p_file_ref    IN  VARCHAR2
                                        ,p_raise_error   IN  VARCHAR2
                                        ,p_rtc           OUT INTEGER);
                            
  PROCEDURE p_bcms_cii_return_animals ( p_file_ref    IN  VARCHAR2
                                       ,p_raise_error   IN  VARCHAR2
                                       ,p_rtc           OUT INTEGER);
                            
  PROCEDURE p_bcms_cii_return_create_clob ( p_file_ref    IN  VARCHAR2
                                           ,p_raise_error IN  VARCHAR2
                                           ,p_rtc         OUT INTEGER);

  PROCEDURE p_bcms_cii_return_pop_results ( p_file_ref    IN  VARCHAR2
                                           ,p_raise_error IN  VARCHAR2
                                           ,p_rtc         OUT INTEGER);

  PROCEDURE p_bcms_cii_returns ( p_inspection_count OUT INTEGER
                                ,p_raise_error      IN  VARCHAR2
                                ,p_rtc              OUT INTEGER);

  PROCEDURE p_update_max_batch_size ( p_batch_size       IN  INTEGER
                                     ,p_raise_error      IN  VARCHAR2
                                     ,p_rtc              OUT INTEGER);

END ai_bcms_cii_returns_pkg;
/
