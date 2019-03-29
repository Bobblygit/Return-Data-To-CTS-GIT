  CREATE OR REPLACE PACKAGE BODY ai_bcms_cii_returns_pkg AS
-- ***********************************************************************************
-- * Program          : ai_bcms_cii_returns_pkg body
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
-- * Iain High        2     04-DEC-2018  Removed all reference to tables inspections_locked and inspection_letter_documents.
-- * Iain High        3     01-FEB-2019  Baseline Version to resume development in Feb 2019.
-- * Iain High        4     05-FEB-2019  Added PROCEDURE p_bcms_cii_remove_inspection.
-- *                                     Added FUNCTION fn_lead_inspector_name.
-- * Iain High        5     05-FEB-2019  Amended PROCEDURE p_log_message changed bcl_i_id to bcl_id.
-- * Iain High        6     07-FEB-2019  Added p_user to PROCEDURE p_bcms_cii_add_inspection
-- *                                     and PROCEDURE p_bcms_cii_pop_inspections.
-- * Iain High        7     10-MAR-2019  Added code to map LIS error code to SIACS error codes.
-- * Iain High        8     15-MAR-2019  Added bcms_cii_inspection_queue to PROCEDURE p_bcms_cii_rollback_last_batch.
-- *                                     Added PROCEDURE p_update_max_batch_size.
-- * Iain High        9     19-MAR-2019  In PROCEDURE p_bcms_cii_returns commented out the call to p_bcms_cii_return_pop_results
-- *                                     because this procedure will now be called by FUSE once CLOB files have been emailed to CTS.
-- * Iain High       10     19-MAR-2019  Renamed PROCEDURE p_bcms_cii_rollback_last_batch to p_bcms_cii_rollback_batch.
-- * Iain High       11     22-MAR-2019  Added 'DEL' to 'AL1-2', 'AL2-2', 'AL3-2', 'ADD-2' queries in PROCEDURE p_bcms_cii_return_animals.
-- *
-- ***********************************************************************************

/*
NOTES
-----

    There are seven tables used in the process of extracting the required data to be sent back to BCMS on a weekly basis.
    BCMS expect 3 files - one for Inspections data , one for Holding Data and one for Animals data.
        
    This process was previously known as the 'Saturday morning job' on the legacy SIACS application as it was run 
    at 0700 hrs each Saturday.
    
    The selection rules for determining which inspections to include in the weekly batch are as follows :-
    
      a) Inspection has completed data capture.
      b) Data for the inspection has NOT already been sent to BCMS (i.e. no records for the inspection in 
         BCMS_CII_RESULTS table.
    
    In summary, the relevant data is first copied to the BCMS_CII_INSPECTION_QUEUE and BCMS_CII_LIVESTOCK tables.
    
    Data is then extracted and converted into the required format and inserted into the following tables :
    
      BCMS_CII_RETURN_INSPECTIONS
      BCMS_CII_RETURN_HOLDINGS
      BCMS_CII_RETURN_ANIMALS
    
    The data in the above three tables is then further processed and inserted into CLOB column BCRD_DATA in the BCMS_CII_RETURN_DATA
    table.
    
    Finally, three records are inserted into the BCMS_CII_RESULTS table for each inspection included in the current run.
    One for Inspections (I), Holdings (H) and Animals (A).
    
    These records are used to ensure that the data for an inspection is only ever extracted and sent to BCMS once.
    
    BCMS_CII_INSPECTION_QUEUE table :-
    This table holds a list of inspections to be included in the current batch.
    The data is purged at the start of the extract process then re-populated with the new data.
    Required for performance reasons.

    BCMS_CII_LIVESTOCK table :-
    This table holds the details of each animal to be included in the current batch.
    The stable structure is identical to that of INSP_ANIMAL_DETAILS table
    The data is purged at the start of the extract process then re-populated with the new data.
    Required for performance reasons.

    BCMS_CII_RETURN_INSPECTIONS table :-
    Contains the extracted data for each inspection in the string format required by BCMS.
    There is one record per inspection.
    
    BCMS_CII_RETURN_HOLDINGS table :-
    Contains the extracted data for each holding in the string format required by BCMS.
    There is one record per holding.

    BCMS_CII_RETURN_ANIMALS table :-
    Contains the extracted data for each animal in the string format required by BCMS.
    There is one record per animal.

    BCMS_CII_RETURN_DATA table :-
    Contains the final version  of the data in its final CLOB format.
    Can contain up to three records per batch (one with file type 'I' inspection, one with file 
    type 'H' holding and one with file type 'A' inspection)
    Note: If there are no animals on the inspection then the 'A' will be missing.

    BCMS_CII_RESULTS table :-
    A control table which is checked by the code to see if data for an inspection has already been returned
    to BCMS. Can contain up to three records per inspection (one with file type 'I' inspection, one with file 
    type 'H' holding and one with file type 'A' inspection)
    Note: If there are no animals on the inspection then the 'A' will be missing.
*/

  pv_err_system    std_err_log.sel_err_system%TYPE  := 'CII';
  pv_err_step_no   std_err_log.sel_err_step_no%TYPE := 0;
  pv_err_module    std_err_log.sel_err_module%TYPE  := 'ai_bcms_cii_returns_pkg'; 

  CURSOR c_get_batch_size IS
  SELECT rc_value
  FROM  ref_codes_all 
  WHERE rc_domain = 'BCMS RETURNS MAX BATCH SIZE';

  CURSOR c_select_batch (cp_batch_size PLS_INTEGER) IS
  SELECT i_id
  FROM (SELECT bciq_i_id i_id
        FROM   bcms_cii_inspection_queue 
        WHERE  bciq_file_ref IS NULL
        AND    bciq_status   IS NULL
        ORDER BY created_date)
  WHERE ROWNUM <= cp_batch_size;

  -- Count the number of inspections which are waiting in the queue.                                
  CURSOR c_get_inspection_count IS 
  SELECT COUNT(*) 
  FROM bcms_cii_inspection_queue
  WHERE bciq_file_ref IS NULL;
  
  -- Count the number of inspections which have been processed in the current batch.                                
  CURSOR c_get_inspections_processed IS 
  SELECT COUNT(*) 
  FROM bcms_cii_return_inspect_fields;

  -- Get the next sequential file_ref                                
  CURSOR c_get_next_file_ref IS
  SELECT TO_CHAR(NVL(MAX(TO_NUMBER(bcr_file_ref)),0) + 1) 
  FROM bcms_cii_results;

  FUNCTION fn_lead_inspector_name ( p_inspection_id IN inspections_all.i_id%TYPE) RETURN CHAR IS
  
    lv_lead_inspector_name present_at_inspection.pai_name%TYPE;
  
    CURSOR c_get_lead_inspector_name IS
    SELECT SUBSTR(pai_name, 1, 1) || SUBSTR(pai_name, INSTR(pai_name, ' ')) 
    FROM   present_at_inspection pai1 
    WHERE  pai1.pai_role = 'E' 
    AND    pai1.pai_i_id = p_inspection_id
    AND    pai1.pai_id = (SELECT MAX(pai2.pai_id) 
                          FROM  present_at_inspection pai2 
                          WHERE pai2.pai_i_id = pai1.pai_i_id
                          AND   pai2.pai_role = 'E' 
                          AND   pai2.pai_name IS NOT NULL);
                          
  BEGIN
  
    OPEN  c_get_lead_inspector_name;
    FETCH c_get_lead_inspector_name INTO lv_lead_inspector_name;
    CLOSE c_get_lead_inspector_name;

    RETURN lv_lead_inspector_name;
    
  END fn_lead_inspector_name;
 
  PROCEDURE p_log_message ( p_message IN bcms_cii_log.bcl_message%TYPE) IS
  -- Note: autonomous transaction so commit here will not affect calling program
  
    PRAGMA AUTONOMOUS_TRANSACTION;
  
  BEGIN
 
    DBMS_OUTPUT.PUT_LINE(p_message);
 
    INSERT INTO bcms_cii_log 
    (bcl_id,
     bcl_message)
    VALUES
    (bcms_cii_log_seq.NEXTVAL,
     p_message);
     
    COMMIT;

  END p_log_message;

  PROCEDURE p_select_next_batch ( p_file_ref         IN  bcms_cii_return_data.bcrd_file_ref%TYPE
                                 ,p_raise_error      IN  VARCHAR2
                                 ,p_rtc              OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Update the maximum number of inspections to be included in BCMS Return Batch
    -- *
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   15-MAR-2019  Initial version
    -- *********************************************************************************************
                                     
    lv_batch_size           PLS_INTEGER;
    lv_inspections_selected PLS_INTEGER := 0;
    lv_raise_error          VARCHAR2(1) := p_raise_error;
    lv_rtc                  NUMBER := 0;

  BEGIN
  
    pv_err_step_no := 555;

    OPEN  c_get_batch_size;
    FETCH c_get_batch_size INTO lv_batch_size;
    CLOSE c_get_batch_size;
    
    IF lv_batch_size BETWEEN 1 AND 5000 THEN

      pv_err_step_no := 560;
    
      pv_err_step_no := 565;

      p_log_message       ('BCMS Return Batch maximum size is ' || TO_CHAR(lv_batch_size));

      pv_err_step_no := 570;

      FOR i IN c_select_batch (lv_batch_size) LOOP

        UPDATE bcms_cii_inspection_queue 
        SET    bciq_file_ref = p_file_ref,
               bciq_status   = 'P'
        WHERE  bciq_i_id     = i.i_id;
        
        lv_inspections_selected := lv_inspections_selected +1;

       END LOOP;

      p_log_message       (lv_inspections_selected || ' inspections selected for processing');

      p_rtc := 0;
      
    ELSE

      p_log_message       ('FAILED to find BCMS Return Batch maximum size');

      pv_err_step_no := 575;
      p_rtc := 1;
           
    END IF;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_select_next_batch'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_select_next_batch;

  PROCEDURE p_bcms_cii_rollback_batch ( p_file_ref      IN  bcms_cii_results.bcr_file_ref%TYPE
                                       ,p_raise_error   IN  VARCHAR2
                                       ,p_rtc           OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Delete all data in the bcms_cii_return_data and bcms_cii_results tables   
    -- *                   from data extracted (but not necessaily) sent to BCMS for all inspections completed in the previous run.
    -- *                   Intended as a development and test utility only.
    -- *
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   01-FEB-2019  Initial version
    -- *********************************************************************************************

    lv_batch    bcms_cii_results.bcr_file_ref%TYPE;
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;

    CURSOR c_get_batch IS
    SELECT 1
    FROM  bcms_cii_return_data
    WHERE bcrd_file_ref = p_file_ref;

  BEGIN

    p_rtc := -1;
    pv_err_step_no := 3000;   
    
    OPEN  c_get_batch;
    FETCH c_get_batch INTO lv_batch;
    CLOSE c_get_batch;

    pv_err_step_no := 3010;   

    IF lv_batch IS NOT NULL THEN 
    
      DELETE FROM bcms_cii_return_data WHERE bcrd_file_ref = p_file_ref;
      pv_err_step_no := 3020;   

      DELETE FROM bcms_cii_results     WHERE bcr_file_ref  = p_file_ref;
      pv_err_step_no := 3030;   
 
      UPDATE bcms_cii_inspection_queue
      SET bciq_file_ref = NULL,
          bciq_status   = NULL
      WHERE (   (bciq_file_ref = p_file_ref)
             OR (bciq_status = 'P'));

      pv_err_step_no := 3040;   
      p_rtc := 0;

    ELSE
      DBMS_OUTPUT.PUT_LINE('ERROR - Could not find batch ' || p_file_ref);
      p_rtc := -1;
    END IF;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_rollback_batch'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_rollback_batch;

  PROCEDURE p_bcms_cii_reset_batch ( p_file_ref      IN  bcms_cii_results.bcr_file_ref%TYPE
                                    ,p_raise_error   IN  VARCHAR2
                                    ,p_rtc           OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Finad all inspection in the queue with status = 'P' and reset the status to NULL;   
    -- *                   This will allow the inspections to be re-processed.
    -- *
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   19-MAR-2019  Initial version
    -- *********************************************************************************************

    lv_batch    bcms_cii_results.bcr_file_ref%TYPE;
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;

    CURSOR c_get_batch IS
    SELECT 1
    FROM  bcms_cii_return_data
    WHERE bcrd_file_ref = p_file_ref;

    CURSOR c_get_inspections_to_requeue IS
    SELECT bciq_i_id i_id
    FROM  bcms_cii_inspection_queue
    WHERE bciq_status = 'P';

  BEGIN

    p_rtc := -1;
    pv_err_step_no := 3000;   
    
    OPEN  c_get_batch;
    FETCH c_get_batch INTO lv_batch;
    CLOSE c_get_batch;

    pv_err_step_no := 3010;   

    IF lv_batch IS NOT NULL THEN 
    
      DELETE FROM bcms_cii_return_data WHERE bcrd_file_ref = p_file_ref;
      pv_err_step_no := 3020;   

      DELETE FROM bcms_cii_results     WHERE bcr_file_ref  = p_file_ref;
      pv_err_step_no := 3030;   
 
      FOR i IN c_get_inspections_to_requeue LOOP 
      
        UPDATE bcms_cii_inspection_queue
        SET bciq_file_ref = NULL,
            bciq_status   = NULL
        WHERE bciq_i_id = i.i_id;

        p_log_message('Inspection ' || i.i_id || ' has been re-queued.');

      END LOOP;

      pv_err_step_no := 3040;   
      p_rtc := 0;

      p_log_message('Failed BATCH ' || p_file_ref || ' has been reset for re-processing.');

    ELSE
      DBMS_OUTPUT.PUT_LINE('ERROR - Could not find batch ' || p_file_ref);
      p_rtc := -1;
    END IF;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_reset_batch'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_reset_batch;
                                            
  PROCEDURE p_bcms_cii_delete_old_tmp_data ( p_raise_error   IN  VARCHAR2
                                            ,p_rtc           OUT INTEGER) IS
                                        
    -- *********************************************************************************************
    -- * Purpose         : Delete all data in the processing tables which will still be there  
    -- *                   from data sent to BCMS for all inspections completed in the previous run.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- * Iain High    2.0   11-MAR-2019  Added bcms_cattle_insp_results.
    -- * Iain High    3.0   13-MAR-2019  Added bcms_cii_return_animal_fields.
    -- *                                 Added bcms_cii_return_holding_fields.
    -- *                                 Added bcms_cii_return_inspect_fields.
    -- *********************************************************************************************
                                
    lv_start_time    DATE := SYSDATE;
    lv_end_time      DATE;
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;
                                        
  BEGIN

    p_rtc := -1;
    
    pv_err_step_no := 1000;   

    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_livestock';

    pv_err_step_no := 1015;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cattle_insp_results';

    pv_err_step_no := 1018;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_return_inspect_fields';

    pv_err_step_no := 1020;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_return_inspections';

    pv_err_step_no := 1025;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_return_holding_fields';

    pv_err_step_no := 1030;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_return_holdings';

    pv_err_step_no := 1035;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_return_animal_fields';

    pv_err_step_no := 1040;   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bcms_cii_return_animals';

    pv_err_step_no := 1050;

    lv_end_time := SYSDATE;

    pv_err_step_no := 1060;
    
    p_log_message('p_bcms_cii_delete_old_tmp_data FINISHED succcessfully. [Elapsed Time '  || LTRIM(TO_CHAR(((lv_end_time  - lv_start_time)*24*60*60), '999,999,999,999')) || ' seconds]');

    pv_err_step_no := 1070;
    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_delete_old_tmp_data'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_delete_old_tmp_data;

  PROCEDURE p_bcms_cii_add_inspection  ( p_inspection_id    IN inspections_all.i_id%TYPE
                                        ,p_user             IN  VARCHAR2
                                        ,p_raise_error      IN  VARCHAR2
                                        ,p_rtc              OUT INTEGER) IS
                                        
    -- *********************************************************************************************
    -- * Purpose         : Add the specified inspection to the list of inspections that   
    -- *                   are waiting to be sent to BCMS in the next batch run.
    -- *
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   01-FEB-2019  Initial version
    -- * Iain High    2.0   07-FEB-2019  Added p_user
    -- * Iain High    3.0   19-MAR-2019  Added  check to ensure that inspection_pack_bus_details record exists for the inspection.
    -- *********************************************************************************************

    lv_raise_error        VARCHAR2(1) := p_raise_error;
    lv_processed          NUMBER := 0;
    lv_queued             NUMBER := 0;
    lv_insp_pack_bus_dets NUMBER := 0;
    lv_rtc                NUMBER := 0;

    CURSOR c_check_already_processed IS
    SELECT 1
    FROM  bcms_cii_results
    WHERE bcr_i_id = p_inspection_id;
    
    CURSOR c_check_already_queued IS
    SELECT 1
    FROM  bcms_cii_inspection_queue
    WHERE bciq_i_id = p_inspection_id;
    
    CURSOR c_check_for_insp_pack_bus_dets IS
    SELECT 1
    FROM  inspection_pack_bus_details
    WHERE ipbd_i_id = p_inspection_id;
    
    inspection_already_processed EXCEPTION; PRAGMA EXCEPTION_INIT(inspection_already_processed, -20037);
    inspection_already_queued    EXCEPTION; PRAGMA EXCEPTION_INIT(inspection_already_queued,    -20038);
    insp_pack_bus_dets_not_found EXCEPTION; PRAGMA EXCEPTION_INIT(insp_pack_bus_dets_not_found, -20054);

  BEGIN
  
    p_rtc := -1;
    pv_err_step_no := 9000;
    
    OPEN  c_check_for_insp_pack_bus_dets;
    FETCH c_check_for_insp_pack_bus_dets INTO lv_insp_pack_bus_dets;
    CLOSE c_check_for_insp_pack_bus_dets;

    pv_err_step_no := 9001;
    
    IF NVL(lv_insp_pack_bus_dets, 0) != 1 THEN
      RAISE insp_pack_bus_dets_not_found;
    END IF;
                                        
    pv_err_step_no := 9003;
    
    OPEN  c_check_already_processed;
    FETCH c_check_already_processed INTO lv_processed;
    CLOSE c_check_already_processed;

    pv_err_step_no := 9005;
    
    IF lv_processed = 1 THEN
      RAISE inspection_already_processed;
    END IF;
                                        
    pv_err_step_no := 9010;
    
    OPEN  c_check_already_queued;
    FETCH c_check_already_queued INTO lv_queued;
    CLOSE c_check_already_queued;
                                        
    pv_err_step_no := 9015;
    
    IF lv_queued = 1 THEN
      RAISE inspection_already_queued;
    END IF;
                                        
    pv_err_step_no := 9020;
    
    INSERT INTO bcms_cii_inspection_queue 
    (bciq_i_id,
     created_by)
    VALUES
    (p_inspection_id,
     p_user);

    pv_err_step_no := 9025;
    p_log_message('p_bcms_cii_add_inspection FINISHED succcessfully. Inspection '  || p_inspection_id || ' inserted into bcms_cii_inspection_queue table.' );
    p_rtc := 0;
    
  EXCEPTION

    WHEN insp_pack_bus_dets_not_found THEN
       p_rtc := ai_err_log_pkg.write_err_log(p_system      => pv_err_system
                                            ,p_err_proc_dt => sysdate
                                            ,p_module      => pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,p_type        => 'F'
                                            ,p_err_no      => 20054
                                            ,p_err_msg     => NULL
                                            ,p_step_no     => pv_err_step_no
                                            ,p_data_values => NULL
                                            ,p_name_1      => 'p_inspection_id'   
                                            ,p_value_1     =>  p_inspection_id
                                            ,p_name_2      => NULL 
                                            ,p_value_2     => NULL
                                            ,p_name_3      => NULL 
                                            ,p_value_3     => NULL
                                            ,p_name_4      => NULL   
                                            ,p_value_4     => NULL);
        IF p_raise_error = 'Y' THEN
          RAISE;
        END IF;

    WHEN inspection_already_processed THEN
       p_rtc := ai_err_log_pkg.write_err_log(p_system      => pv_err_system
                                            ,p_err_proc_dt => sysdate
                                            ,p_module      => pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,p_type        => 'F'
                                            ,p_err_no      => 20037
                                            ,p_err_msg     => NULL
                                            ,p_step_no     => pv_err_step_no
                                            ,p_data_values => NULL
                                            ,p_name_1      => 'p_inspection_id'   
                                            ,p_value_1     =>  p_inspection_id
                                            ,p_name_2      => NULL 
                                            ,p_value_2     => NULL
                                            ,p_name_3      => NULL 
                                            ,p_value_3     => NULL
                                            ,p_name_4      => NULL   
                                            ,p_value_4     => NULL);
        IF p_raise_error = 'Y' THEN
          RAISE;
        END IF;

    WHEN inspection_already_queued THEN
       p_rtc := ai_err_log_pkg.write_err_log(p_system      => pv_err_system
                                            ,p_err_proc_dt => sysdate
                                            ,p_module      => pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,p_type        => 'F'
                                            ,p_err_no      => 20038
                                            ,p_err_msg     => NULL
                                            ,p_step_no     => pv_err_step_no
                                            ,p_data_values => NULL
                                            ,p_name_1      => 'p_inspection_id'   
                                            ,p_value_1     =>  p_inspection_id
                                            ,p_name_2      => NULL 
                                            ,p_value_2     => NULL
                                            ,p_name_3      => NULL 
                                            ,p_value_3     => NULL
                                            ,p_name_4      => NULL   
                                            ,p_value_4     => NULL);
        IF p_raise_error = 'Y' THEN
          RAISE;
        END IF;

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_add_inspection;
    
  PROCEDURE p_bcms_cii_remove_inspection  ( p_inspection_id    IN inspections_all.i_id%TYPE
                                           ,p_raise_error      IN  VARCHAR2
                                           ,p_rtc              OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Add the specified inspection to the list of inspections that   
    -- *                   are waiting to be sent to BCMS in the next batch run.
    -- *
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   01-FEB-2019  Initial version
    -- *********************************************************************************************

    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_processed     NUMBER := 0;
    lv_queued        NUMBER := 0;
    lv_rtc           NUMBER := 0;

    CURSOR c_check_already_queued IS
    SELECT 1
    FROM  bcms_cii_inspection_queue
    WHERE bciq_i_id = p_inspection_id;
    
    CURSOR c_check_already_processed IS
    SELECT 1
    FROM  bcms_cii_inspection_queue
    WHERE bciq_i_id = p_inspection_id
    AND   bciq_file_ref IS NOT NULL;
    
    inspection_not_queued         EXCEPTION; PRAGMA EXCEPTION_INIT(inspection_not_queued,         -20039);
    inspection_has_been_processed EXCEPTION; PRAGMA EXCEPTION_INIT(inspection_has_been_processed, -20040);

  BEGIN
  
    p_rtc := -1;
    pv_err_step_no := 10000;

    OPEN  c_check_already_queued;
    FETCH c_check_already_queued INTO lv_queued;
    
      IF c_check_already_queued%NOTFOUND THEN
        lv_queued := -1;
      END IF;
      
    CLOSE c_check_already_queued;
                                        
    pv_err_step_no := 10010;
    
    IF lv_queued = -1 THEN
      RAISE inspection_not_queued;
    END IF;
                                        
    pv_err_step_no := 10020;
    
    OPEN  c_check_already_processed;
    FETCH c_check_already_processed INTO lv_processed;
    CLOSE c_check_already_processed;

    pv_err_step_no := 10030;
    
    IF lv_processed = 1 THEN
      RAISE inspection_has_been_processed;
    END IF;
                                        
    pv_err_step_no := 10040;
    
    DELETE FROM bcms_cii_inspection_queue
    WHERE bciq_i_id = p_inspection_id
    AND ROWNUM = 1;

    pv_err_step_no := 10050;

    p_log_message('p_bcms_cii_removed_inspection FINISHED succcessfully. Inspection '  || p_inspection_id || ' removed from bcms_cii_inspection_queue table.' );

    pv_err_step_no := 10060;
    p_rtc := 0;
    
  EXCEPTION

    WHEN inspection_not_queued THEN
       p_rtc := ai_err_log_pkg.write_err_log(p_system      => pv_err_system
                                            ,p_err_proc_dt => sysdate
                                            ,p_module      => pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,p_type        => 'F'
                                            ,p_err_no      => 20039
                                            ,p_err_msg     => NULL
                                            ,p_step_no     => pv_err_step_no
                                            ,p_data_values => NULL
                                            ,p_name_1      => 'p_inspection_id'   
                                            ,p_value_1     =>  p_inspection_id
                                            ,p_name_2      => NULL 
                                            ,p_value_2     => NULL
                                            ,p_name_3      => NULL 
                                            ,p_value_3     => NULL
                                            ,p_name_4      => NULL   
                                            ,p_value_4     => NULL);
        IF p_raise_error = 'Y' THEN
          RAISE;
        END IF;

    WHEN inspection_has_been_processed THEN
       p_rtc := ai_err_log_pkg.write_err_log(p_system      => pv_err_system
                                            ,p_err_proc_dt => sysdate
                                            ,p_module      => pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,p_type        => 'F'
                                            ,p_err_no      => 20040
                                            ,p_err_msg     => NULL
                                            ,p_step_no     => pv_err_step_no
                                            ,p_data_values => NULL
                                            ,p_name_1      => 'p_inspection_id'   
                                            ,p_value_1     =>  p_inspection_id
                                            ,p_name_2      => NULL 
                                            ,p_value_2     => NULL
                                            ,p_name_3      => NULL 
                                            ,p_value_3     => NULL
                                            ,p_name_4      => NULL   
                                            ,p_value_4     => NULL);
        IF p_raise_error = 'Y' THEN
          RAISE;
        END IF;

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_add_inspection'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;
      
  END p_bcms_cii_remove_inspection;

  PROCEDURE p_bcms_cii_pop_inspections ( p_inspection_count IN  INTEGER
                                        ,p_user             IN  VARCHAR2
                                        ,p_raise_error      IN  VARCHAR2
                                        ,p_rtc              OUT INTEGER) IS
                                        
    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_INSPECTION_QUEUE table with candidate  
    -- *                   inspections for the current run. Number of rows to be inserted
    -- *                   is limited to a max of p_inspection_count
    -- *                   Intended as a development and test utility only.
    
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- * Iain High    2.0   07-FEB-2019  Added p_user
    -- *********************************************************************************************
                                
    lv_raise_error       VARCHAR2(1) := p_raise_error;
    lv_inspection_count  NUMBER := 0;
    lv_rtc               NUMBER := 0;

    CURSOR c_get_inspections IS
    SELECT i_id insp_id
    FROM   inspections_all i
    WHERE  i_type = 'LI'
    AND    i_status = 'C1'
    AND    i_id >= 201614
    AND    i_insp_capture_complete_ind = 'Y'
    AND    i_id IN(SELECT rl_i_id
                   FROM requested_locations)
    AND    NOT EXISTS(SELECT 1
                     FROM bcms_cii_results
                     WHERE bcr_i_id = i.i_id
                     AND   bcr_file_type = 'I')
    AND    NOT EXISTS(SELECT 1
                     FROM bcms_cii_inspection_queue
                     WHERE bciq_i_id = i.i_id)
    AND ROWNUM <= p_inspection_count;
                                        
  BEGIN
    p_rtc := -1;
    pv_err_step_no := 8000;
    
    FOR j IN c_get_inspections LOOP

      p_bcms_cii_add_inspection( p_inspection_id      => j.insp_id
                                ,p_user               => p_user
                                ,p_raise_error        => lv_raise_error
                                ,p_rtc                => lv_rtc);

      p_log_message('Added inspection '  || j.insp_id);
    
    END LOOP;
                   
    pv_err_step_no := 8350;
    lv_inspection_count := SQL%ROWCOUNT;                  
               
    pv_err_step_no := 8400;
    p_log_message('p_bcms_cii_pop_inspections FINISHED succcessfully.'  || lv_inspection_count || ' records inserted into bcms_cii_inspection_queue table.' );
    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_pop_inspections'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_pop_inspections;

  PROCEDURE p_bcms_cii_pop_livestock   ( p_raise_error   IN  VARCHAR2
                                        ,p_rtc           OUT INTEGER) IS
                                        
    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_LIVESTOCK table with a list of animals  
    -- *                   for the current run.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************
                                
    lv_start_time    DATE := SYSDATE;
    lv_end_time      DATE;
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;
                                        
  BEGIN
    p_rtc := -1;
    pv_err_step_no := 8500;

    INSERT INTO bcms_cii_livestock
    SELECT
      created_date, 
      created_by, 
      iad_i_id, 
      iad_asa_id, 
      iad_ab_id, 
      iad_eartag_no, 
      iad_country, 
      iad_herd, 
      iad_cow_no, 
      iad_checksum, 
      iad_dob, 
      iad_end_retention_date, 
      iad_cow, 
      iad_location_code, 
      iad_date_moved_on, 
      iad_date_moved_off, 
      iad_date_of_death, 
      iad_sex, 
      iad_breed, 
      iad_passport_version, 
      iad_dam_ear_tag, 
      iad_surrogate_eartag, 
      iad_scheme, 
      iad_passport_seen, 
      iad_animal_seen, 
      iad_records_seen, 
      iad_foreign_animal, 
      iad_direction, 
      iad_first_calving_date, 
      iad_latest_calving_date, 
      iad_cii_code, 
      iad_seq_no, 
      iad_comm, 
      iad_batch_no, 
      iad_management_tag, 
      iad_mts, 
      iad_dts, 
      iad_mt, 
      iad_cts_passport_seen, 
      iad_cts_passport_or, 
      iad_passport_or, 
      iad_cor_seen, 
      iad_cor_or, 
      iad_ccd_seen, 
      iad_ccd_or, 
      iad_docs_retained, 
      iad_docs_or, 
      iad_all_docs_seen, 
      iad_records_discrep, 
      iad_animal_discrep, 
      iad_pass_refused_ind, 
      iad_retag_ind, 
      iad_on_confidence, 
      iad_off_confidence, 
      iad_kill_number, 
      iad_death_location, 
      iad_death_notified, 
      iad_old_passport_seen, 
      iad_old_passport_or, 
      iad_old_passport_expected, 
      iad_cts_passport_expected, 
      iad_cor_expected, 
      iad_ccd_expected, 
      iad_sire_id, 
      iad_type, 
      iad_nor_expected, 
      iad_nor_seen, 
      iad_nor_or, 
      iad_dob_notified, 
      iad_mvon_notified, 
      iad_mvoff_notified, 
      iad_dod_notified, 
      iad_prev_loccode, 
      iad_next_loccode, 
      iad_id, 
      iad_annex_rule_code 
    FROM insp_animal_details
    WHERE iad_i_id IN 
             (SELECT i_id 
              FROM   bcms_cii_return_inspect_fields);
    lv_end_time := SYSDATE;

    pv_err_step_no := 8600;
    p_log_message('p_bcms_cii_pop_livestock FINISHED succcessfully.'  || LTRIM(TO_CHAR(SQL%ROWCOUNT, '999,999,999,999')) || ' records inserted into bcms_cii_livestock table. ' ||
                  ' [Elapsed Time '  || LTRIM(TO_CHAR(((lv_end_time  - lv_start_time)*24*60*60), '999,999,999,999')) || ' seconds]');
               
    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_pop_livestock'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_pop_livestock;


  PROCEDURE p_bcms_cii_pop_cattle_insp_res   ( p_raise_error   IN  VARCHAR2
                                                  ,p_rtc           OUT INTEGER) IS
                                        
    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CATTLE_INSP_RESULTS table with the current set of LIS error codes  
    -- *                   for the current run. Then remove error codes which are specific to LIS and then
    -- *                   map the remaining LIS Error Code to the SIACS Error Code where required.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   11-MAR-2019  Initial version
    -- *********************************************************************************************
                                
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;

    -- Required Mapping from LIS to SIACS Error Codes
    /*
    -- LIS      SIACS
    ------      -----
    'FB',        'FM'
    'FD',        'DD'
    'FM',        'FM'
    'SM-FM',     -
    'IB',        'DB'
    'ID',        'MV'
    'IDAM',      'ID'
    'IM',        'MV'
    'SM-IM',     -
    'ISB',       'OP'
    'LB',        -
    'LD',        -
    'LM',        -
    'SM-LM',     -
    'LR',        -
    'SM-LR',     -
    'NA',        'NA'
    'NF',        'NF'
    'SM-NF',     -
    'NP',        'NP'
    'TG1',       'TG1'
    'TG2',       'TG2'
    'TG3',       'TG3'
    */
                                        
  BEGIN
    p_rtc := -1;
    pv_err_step_no := 8800;

    -- Insert the current LIS Error Codes.
    INSERT INTO bcms_cattle_insp_results
    SELECT
      created_date,
      created_by,
      cir_ab_id,
      cir_code,
      cir_domain,
      cir_i_id,
      cir_ib_id,
      cir_insp_comment,
      cir_asa_id,
      cir_bov_eartag,
      cir_hh_field,
      cir_location_code,
      cir_iad_id
    FROM cattle_insp_results
    WHERE cir_i_id IN (SELECT i_id 
                       FROM   bcms_cii_return_inspect_fields);         

    pv_err_step_no := 8810;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' records inserted into bcms_cattle_insp_results table.' );

     -- Now delete all error codes from BCMS_CATTLE_INSP_RESULTS that are specific to LIS and have no relevance to BCMS/CTS.         
    DELETE FROM bcms_cattle_insp_results WHERE cir_code IN
     ('SM-FM', 
      'SM-IM', 
      'LB',   
      'LD',  
      'LM',  
      'SM-LM',
      'LR',
      'SM-LR',
      'SM-NF',
      'RES',
      'ORNR',
      'ORD',
      'NORD',
      'APP',
      'NAPP');
      
    pv_err_step_no := 8820;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS-only records deleted from bcms_cattle_insp_results table.' );

    -- Now convert the following LIS Codes to their SIACS equivalent.

    --  LIS        SIACS
    --------       -----
    -- 'FB',        'FM'
    -- 'FD',        'DD'
    -- 'IB',        'DB'
    -- 'ID',        'MV'
    -- 'IDAM',      'ID'
    -- 'IM',        'MV'
    -- 'ISB',       'OP'

    UPDATE bcms_cattle_insp_results SET cir_code = 'FM'  WHERE cir_code = 'FB';      
    pv_err_step_no := 8821;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''FB'' Codes converted to ''FM'' in bcms_cattle_insp_results table.' );

    UPDATE bcms_cattle_insp_results SET cir_code = 'DD'  WHERE cir_code = 'FD';      
    pv_err_step_no := 8822;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''FD'' Codes converted to ''DD'' in bcms_cattle_insp_results table.' );

    UPDATE bcms_cattle_insp_results SET cir_code = 'DB'  WHERE cir_code = 'IB';     
    pv_err_step_no := 8823;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''IB'' Codes converted to ''DB'' in bcms_cattle_insp_results table.' );

    UPDATE bcms_cattle_insp_results SET cir_code = 'MV'  WHERE cir_code = 'ID';
    pv_err_step_no := 8824;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''ID'' Codes converted to ''MV'' in bcms_cattle_insp_results table.' );

    UPDATE bcms_cattle_insp_results SET cir_code = 'ID'  WHERE cir_code = 'IDAM';
    pv_err_step_no := 8825;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''IDAM'' Codes converted to ''ID'' in bcms_cattle_insp_results table.' );

    UPDATE bcms_cattle_insp_results SET cir_code = 'MV'  WHERE cir_code = 'IM';
    pv_err_step_no := 8826;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''IM'' Codes converted to ''MV'' in bcms_cattle_insp_results table.' );

    UPDATE bcms_cattle_insp_results SET cir_code = 'OP'  WHERE cir_code = 'ISB';
    pv_err_step_no := 8827;
    p_log_message('p_bcms_cii_pop_cattle_insp_res RUNNING. '  || SQL%ROWCOUNT || ' LIS ''ISB'' Codes converted to ''OP'' in bcms_cattle_insp_results table.' );

               
    pv_err_step_no := 8890;
    p_log_message('p_bcms_cii_pop_cattle_insp_res FINISHED succcessfully. LIS Codes converted to their SIACS equivalent in bcms_cattle_insp_results table where required.' );
    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_pop_cattle_insp_res'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_pop_cattle_insp_res;

  PROCEDURE p_bcms_cii_return_inspections ( p_file_ref    IN  VARCHAR2
                                           ,p_raise_error   IN  VARCHAR2
                                           ,p_rtc           OUT INTEGER) IS

    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_RETURN_INSPECTIONS table with formatted inspection  
    -- *                   data for the current run.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************
                                
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;

  BEGIN
  
    p_rtc := -1;
    pv_err_step_no := 3000;

  INSERT INTO bcms_cii_return_inspect_fields
  SELECT  i_id
         , 'SIACS'
         || TO_CHAR (SYSDATE, 'YYYY')
         || TO_CHAR (SYSDATE, 'DDD')
         || 'S'
         || p_file_ref
         || 'ins'
         , 'D'
         , ROWNUM
         , 'I'
         , 'S' || i_id
         , DECODE (GREATEST (TRUNC (SYSDATE),
                                 '02-AUG-'
                              || TO_CHAR (SYSDATE, 'YYYY') ),
                    TRUNC (SYSDATE), TO_CHAR (SYSDATE, 'YYYY'),
                      TO_CHAR (SYSDATE, 'YYYY')
                    - 1)
         , 'CII'
         , i_inspected_date
         , i_insp_completed_date
         , DECODE (i_type_x,
                    NULL, i_type_m,
                    i_type_x)
         , DECODE(ipbd.ipbd_area_office
         -- LIS    BAU                   Area Office Name       
          , 1   ,   18                   --  Ayr                         
          , 2   ,   22                   --  Benbecula                   
          , 3   ,   19                   --  Dumfries                    
          , 4   ,   25                   --  Elgin                       
          , 5   ,   15                   --  Galashiels                  
          , 6   ,   26                   --  Golspie                     
          , 7   ,   28                   --  Hamilton                    
          , 8   ,   14                   --  Inverness                   
          , 9   ,   20                   --  Kirkwall                    
          ,10   ,   27                   --  Lerwick                     
          ,11   ,    9                   --  Oban                        
          ,12   ,   21                   --  Perth                       
          ,13   ,   24                   --  Portree                     
          ,14   ,   10                   --  Stornoway                   
          ,15   ,   17                   --  Thurso
          ,16   ,   16                   --  Inverurie                   
          ,0) 
         , CEIL (  SUM (  (SELECT   COUNT (DISTINCT (iad_eartag_no) )
                                FROM insp_animal_details
                               WHERE iad_i_id = i_id
                                 AND iad_direction = 'FROM'
                                 AND iad_animal_seen = 'Y'
                            GROUP BY iad_i_id)
                         * 10)
                  / 60)
         , fn_lead_inspector_name(i.i_id)
         , i_c_notice
         , (SELECT answer FROM vw_lis_questions_and_answers WHERE insp_id = i_id AND aq_id=70000330)
    FROM inspections_all i
    ,    inspection_pack_bus_details ipbd
    WHERE ipbd.ipbd_i_id = i.i_id 
    AND  i.i_id IN (SELECT bciq_i_id FROM bcms_cii_inspection_queue WHERE bciq_status = 'P')
    GROUP BY i_id,
         ipbd_area_office,
         ROWNUM,
         TO_CHAR (SYSDATE, 'YYYY'),
         TO_CHAR (SYSDATE, 'DAY'),
         TO_CHAR (i_inspected_date, 'YYYY'),
         i_inspected_date,
         i_insp_completed_date,
         i_time_taken,
         i_c_notice,
         i_type_x,
         i_type_m;

    INSERT INTO bcms_cii_return_inspections
    (bcri_line_no,
     bcri_inspection_detail)
    SELECT ins3,
           ins1 || '|' ||
           ins2 || '|' ||
           ins3 || '|' ||
           ins4 || '|' ||
           ins5 || '|' ||
           ins6 || '|' ||
           ins7 || '|' ||
           ins8 || '|' ||
           ins9 || '|' ||
          ins10 || '|' ||
          ins11 || '|' ||
          ins12 || '|' ||
          ins13 || '|' ||
          ins14
    FROM bcms_cii_return_inspect_fields;

    pv_err_step_no := 3010;

    p_log_message('p_bcms_cii_return_inspections FINISHED succcessfully.'  || SQL%ROWCOUNT || ' records inserted into bcms_cii_return_inspections table.' );
    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_return_inspections'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_return_inspections;
                            
  PROCEDURE p_bcms_cii_return_holdings ( p_file_ref    IN  VARCHAR2
                                        ,p_raise_error   IN  VARCHAR2
                                        ,p_rtc           OUT INTEGER) IS

    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_RETURN_HOLDINGS table with formatted holding  
    -- *                   data for the current run.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************
                                
    lv_start_time    DATE := SYSDATE;
    lv_end_time      DATE;
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;

  BEGIN

    p_rtc := -1;
    pv_err_step_no := 4000;
  
    INSERT INTO bcms_cii_return_holding_fields
    SELECT
       v0_i_id
      ,v0_rl_location_code
	    ,'SIACS'
      ||to_char(sysdate,'YYYY')
      ||to_char(sysdate,'DDD')
      ||'S'
      ||p_file_ref
      ||'hol'
      ,'D'
      ,ROWNUM
      ,'I'
      ,'S' || v0_i_id
      ,(SELECT DECODE(GREATEST(TRUNC(SYSDATE)
                                       ,'02-AUG-'||TO_CHAR(SYSDATE,'YYYY'))
                       , TRUNC(SYSDATE), TO_CHAR(SYSDATE,'YYYY')
                       , TO_CHAR(SYSDATE,'YYYY') -1
                       ) FROM dual) insp_year
      ,v0_rl_location_code
      ,NULL
      ,map_ref
      ,nvl(comments,NULL)
      ,NULL
      ,decode(i_etas_unused_et_secure,'No','Yes',NULL)
      ,v7_type
      ,contact_name
      ,addr1
      ,addr2
      ,addr3
      ,addr4
      ,post_code
      ,phone_no
      ,i_10pc_double_tag_check
      ,nvl(animals_seen,0)
      ,nvl(animals_seen_with_cii,0)
      ,nvl(animals_seen_with_cii,0)
      ,nvl(decode(animals_seen,0,0,round(animals_seen_with_cii/animals_seen*100)),0)
      ,NULL
      ,NULL
      ,nvl(records_checked,0)
      ,nvl(animals_with_nf,0)
      ,'Yes'
      ,nvl(decode(animals_seen+records_checked,0,0,round((animals_seen_with_cii+animals_with_nf)/
                    (animals_seen+records_checked)*100)),0)
      ,decode(rl_location_type, 'Main Location Code', 'Y', 'Main Farm Code', 'Y', NULL)
      ,rl_location_type col1
    FROM 
       (SELECT i_id                                                               v7_i_id
              ,NULL                                                               v7_type
        FROM inspections_all
        WHERE i_type = 'LI'
        AND i_insp_capture_complete_ind = 'Y'
        AND i_status = 'C1'
        AND NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = i_id
                        AND   bcr_file_type = 'H')
        )
  ,     (SELECT substr(REPLACE(REPLACE(coma_text,chr(10),' '),'|',' '),1,1000)    comments
              ,i_id                                                               v6_i_id
              ,rl_location_code                                                   v6_loc
        FROM comments_all
        ,    requested_locations
        ,    inspections_all
        WHERE i_type = 'LI'
        AND i_insp_capture_complete_ind = 'Y'
        AND i_status = 'C1'
        AND NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = i_id
                        AND   bcr_file_type = 'H')
        AND rl_i_id = i_id
        AND coma_id = i_insp_comm)
      ,(SELECT COUNT(DISTINCT(cir_bov_eartag))                                    animals_with_nf
        ,iad_location_code                                                        v4_loc
        ,iad_i_id                                                                 v4_i_id
        FROM cattle_insp_results cir
        ,    bcms_cii_livestock
        ,    inspections_all
        WHERE i_type = 'LI'
        AND i_insp_capture_complete_ind = 'Y'
        AND i_status = 'C1'
        AND NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = i_id
                        AND   bcr_file_type = 'H')
        AND   iad_i_id = i_id
        AND   cir_i_id = i_id
        AND   cir_domain = 'CII IRREGULARITY CODES'
        AND   cir_bov_eartag = iad_eartag_no
        AND   cir_code NOT IN('DEL','OR','OE')
        AND   iad_direction = 'FROM'
        AND   (iad_date_moved_off IS NOT NULL OR iad_date_of_death IS NOT NULL)
        AND   NOT EXISTS(SELECT 'x'
                         FROM  cattle_insp_results cir2
                         WHERE cir2.cir_i_id = cir.cir_i_id
                         AND   cir2.cir_bov_eartag = cir.cir_bov_eartag
                         AND   cir2.cir_hh_field = cir.cir_hh_field
                         AND   nvl(cir2.cir_location_code,
                                 nvl(cir.cir_location_code, '1234567890123'))
                               = nvl(cir.cir_location_code, '1234567890123')
                          AND   cir2.cir_code IN('OR','OE'))
        GROUP BY iad_location_code,iad_i_id)
      ,(SELECT COUNT(DISTINCT(decode(iad_animal_seen,'Y',iad_eartag_no,NULL)))    animals_seen_with_cii
        , iad_location_code                                                       v3_loc
        , iad_i_id                                                                v3_i_id
        FROM cattle_insp_results cir
        ,    bcms_cii_livestock
        ,    inspections_all
        WHERE i_type = 'LI'
        AND i_insp_capture_complete_ind = 'Y'
        AND i_status = 'C1'
        AND NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = i_id
                        AND   bcr_file_type = 'H')
        AND   iad_i_id = i_id
        AND   iad_date_moved_off IS NULL
        AND   iad_date_of_death IS NULL
        AND   cir_i_id = i_id
        AND   cir_domain = 'CII IRREGULARITY CODES'
        AND   cir_bov_eartag = iad_eartag_no
        AND   cir_code NOT IN('DEL','OR','OE')
        AND   iad_direction = 'FROM'
        AND   NOT EXISTS(SELECT 'x'
                         FROM cattle_insp_results cir2
                         WHERE cir2.cir_i_id = cir.cir_i_id
                         AND   cir2.cir_bov_eartag = cir.cir_bov_eartag
                         AND   cir2.cir_hh_field = cir.cir_hh_field
                         AND   nvl(cir2.cir_location_code,
                                 nvl(cir.cir_location_code, '1234567890123'))
                               = nvl(cir.cir_location_code, '1234567890123')
                         AND   cir2.cir_code IN('OR','OE'))
        GROUP BY iad_location_code,iad_i_id)
      ,(SELECT COUNT(DISTINCT(decode(iad_records_seen,'Y',iad_eartag_no,NULL)))   records_checked
        , iad_location_code                                                       v2_loc
        , iad_i_id                                                                v2_i_id
        FROM bcms_cii_livestock fromrec
        ,    inspections_all
        WHERE iad_direction = 'FROM'
        AND (iad_date_moved_off IS NOT NULL OR iad_date_of_death IS NOT NULL)
        AND iad_i_id = i_id
        AND i_type = 'LI'
        AND i_insp_capture_complete_ind = 'Y'
        AND i_status = 'C1'
        AND NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = i_id
                        AND   bcr_file_type = 'H')
        AND   NOT EXISTS(SELECT 'x'
                         FROM cattle_insp_results
                         WHERE cir_i_id = i_id
                         AND   cir_code = 'DEL'
                         AND   cir_bov_eartag = fromrec.iad_eartag_no
                         AND   nvl(cir_location_code,
                                  nvl(fromrec.iad_location_code, '1234567890123'))
                               = nvl(fromrec.iad_location_code, '1234567890123')
                         AND   cir_domain = 'CII IRREGULARITY CODES')
        GROUP BY iad_location_code,iad_i_id)
      ,(SELECT COUNT(DISTINCT(decode(iad_animal_seen,'Y',iad_eartag_no,NULL)))    animals_seen
        , iad_location_code                                                       v1_loc
        , iad_i_id                                                                v1_i_id
        FROM bcms_cii_livestock fromrec
        ,    inspections_all
        WHERE iad_direction = 'FROM'
        AND   iad_date_moved_off IS NULL
        AND   iad_date_of_death  IS NULL
        AND   iad_i_id = i_id
        AND   i_type = 'LI'
        AND   i_insp_capture_complete_ind = 'Y'
        AND   i_status = 'C1'
        AND NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = i_id
                        AND   bcr_file_type = 'H')
        AND   NOT EXISTS(SELECT 'x'
                         FROM cattle_insp_results cir
                         WHERE cir_i_id = i_id
                         AND   cir_code = 'DEL'
                         AND   cir_bov_eartag = fromrec.iad_eartag_no
                         AND   nvl(cir_location_code, 
                                 nvl(fromrec.iad_location_code, '1234567890123'))
                               = nvl(fromrec.iad_location_code, '1234567890123')
                         AND   cir_domain = 'CII IRREGULARITY CODES')
        GROUP BY iad_location_code,iad_i_id)
        -- 
        -- THIS IS THE DRIVING QUERY - ALL OTHER SUB-QUERIES ARE OUT-JOINED TO THIS ONE.
        --
        ,(SELECT ia.i_id                                                             v0_i_id,
                 rl.rl_location_code                                                 v0_rl_location_code,
                 rl.rl_location_type,
                 ia.i_10pc_double_tag_check                                          i_10pc_double_tag_check,
                 bcrif.i_etas_unused_et_secure                                       i_etas_unused_et_secure
           FROM requested_locations            rl
           ,    bcms_cii_inspection_queue      bciq
           ,    inspections_all                ia 
           ,    bcms_cii_return_inspect_fields bcrif
           WHERE rl.rl_i_id  = bciq.bciq_i_id
           AND   ia.i_id     = bciq.bciq_i_id
           AND   bcrif.i_id     = bciq.bciq_i_id
           AND   bciq_status = 'P')
              ,(SELECT  
                  rcr_i_id                                              v5_i_id,
                  REPLACE(SUBSTR(rcr_c_loccode, 4), '/')                v5_loc,
                  rcr_c_contact                                         contact_name, 
                  rcr_c_loc                                             location_name, 
                  rcr_c_addr1                                           addr1, 
                  rcr_c_addr2                                           addr2, 
                  rcr_c_addr3                                           addr3, 
                  rcr_c_addr4                                           addr4, 
                  rcr_c_pcode                                           post_code, 
                  rcr_c_tel                                             phone_no, 
                  rcr_c_mobile                                          mobile_no, 
                  rcr_c_fax                                             fax, 
                  rcr_c_email                                           email, 
                	rcr_c_mapref                                          map_ref 
           FROM returned_cii_request
           ,    bcms_cii_inspection_queue
           ,    inspections_all
           WHERE rcr_i_id  = bciq_i_id
           AND   i_id      = bciq_i_id
           AND   bciq_status = 'P')  
    WHERE v1_i_id(+) = v0_i_id
    AND   v1_loc(+)  = v0_rl_location_code
    AND   v2_i_id(+) = v0_i_id
    AND   v2_loc(+)  = v0_rl_location_code
    AND   v3_i_id(+) = v0_i_id
    AND   v3_loc(+)  = v0_rl_location_code
    AND   v4_i_id(+) = v0_i_id
    AND   v4_loc(+)  = v0_rl_location_code
    AND   v5_i_id(+) = v0_i_id
    AND   v5_loc(+)  = v0_rl_location_code
    AND   v6_i_id(+) = v0_i_id
    AND   v6_loc(+)  = v0_rl_location_code
    AND   v7_i_id(+) = v0_i_id
    AND   v0_i_id IN (SELECT i_id 
                      FROM   bcms_cii_return_inspect_fields);
                      
    pv_err_step_no := 4008;

    INSERT INTO bcms_cii_return_holdings
    (bcrh_line_no,
     bcrh_holding_detail)
    SELECT hol3, 
       hol1 || '|' ||
       hol2 || '|' ||
       hol3 || '|' ||
       hol4 || '|' ||
       hol5 || '|' ||
       hol6 || '|' ||
       hol7 || '|' ||
       hol8 || '|' ||
       hol9 || '|' ||
      hol10 || '|' ||
      hol11 || '|' ||
      hol12 || '|' ||
      hol13 || '|' ||
      hol14 || '|' ||
      hol15 || '|' ||
      hol16 || '|' ||
      hol17 || '|' ||
      hol18 || '|' ||
      hol19 || '|' ||
      hol20 || '|' ||
      hol21 || '|' ||
      hol22 || '|' ||
      hol23 || '|' ||
      hol24 || '|' ||
      hol25 || '|' ||
      hol26 || '|' ||
      hol27 || '|' ||
      hol28 || '|' ||
      hol29 || '|' ||
      hol30 || '|' ||
      hol31 || '|' ||
      hol32 || '|' ||
      hol33
    FROM bcms_cii_return_holding_fields;

    lv_end_time := SYSDATE;

    pv_err_step_no := 4010;
    p_log_message('p_bcms_cii_return_holdings FINISHED succcessfully.'  || LTRIM(TO_CHAR(SQL%ROWCOUNT, '999,999,999,999')) || ' records inserted into bcms_cii_return_holdings table. ' ||
                  ' [Elapsed Time '  || LTRIM(TO_CHAR(((lv_end_time  - lv_start_time)*24*60*60), '999,999,999,999')) || ' seconds]');

    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_return_holdings'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_return_holdings;
                            
  PROCEDURE p_bcms_cii_return_animals ( p_file_ref      IN  VARCHAR2
                                       ,p_raise_error   IN  VARCHAR2
                                       ,p_rtc           OUT INTEGER) IS

    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_RETURN_ANIMALS table with formatted animal  
    -- *                   data for the current run.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************

    -- In the INSERT statement below you will find 8-off UNION statements each of which can be identified
    -- by their value of 'query_cts_list' :-
    -- AL1-1 - Data originates from the RCR_ANIMALS_PRESENT table (aka 'List 1') and animals have error codes.
    -- AL2-1 - Data originates from the RCR_BORN_GONE table       (aka 'List 2') and animals have error codes.
    -- AL3-1 - Data originates from the RCR_NOTBORN_GONE table    (aka 'List 3') and animals have error codes.
    -- ADD-1 - Additional animals not received from CTS                          and animals have error codes.
    -- AL1-2 - Data originates from the RCR_ANIMALS_PRESENT table (aka 'List 1') and animals have no error codes.
    -- AL2-2 - Data originates from the RCR_BORN_GONE table       (aka 'List 2') and animals have no error codes.
    -- AL3-2 - Data originates from the RCR_NOTBORN_GONE table    (aka 'List 3') and animals have no error codes.
    -- ADD-2 - Additional animals not received from CTS                          and animals have no error codes.
                                
    lv_start_time    DATE := SYSDATE;
    lv_end_time      DATE;
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;
                            
  kount PLS_INTEGER := 0;                            

   TYPE animal_rt IS RECORD (
       row_no PLS_INTEGER,
       animal VARCHAR2(4000)
       );  

   TYPE animal_tt IS TABLE OF animal_rt  
      INDEX BY PLS_INTEGER;  
  
   animals animal_tt;  


BEGIN

    p_rtc := -1;
    pv_err_step_no := 5000;

  INSERT INTO bcms_cii_return_animal_fields
    SELECT  /*+ USE_HASH (inspections_all, t1, t2) */
    i_id, 
    to_location, 
    to_animal,
    ROWNUM,
    query_cts_list,
    from_iad_id,
    'SIACS'
    ||to_char(SYSDATE,'YYYY')
    ||to_char(SYSDATE,'DDD')
    ||'S'
    ||p_file_ref
    ||'ani'
    ,'D'
    ,ROWNUM
    ,'I'
    ,'S' || i_id
    ,(SELECT decode(greatest(trunc(SYSDATE)
                                 ,'02-AUG-'||to_char(SYSDATE,'YYYY'))
                 , trunc(SYSDATE), to_char(SYSDATE,'YYYY')
                 , to_char(SYSDATE,'YYYY') -1
                 ) FROM dual)insp_year
    ,to_location
    ,to_animal
    ,cts_list
    ,cts_type
    ,decode(cts_list,'ADD',NULL,to_dob)
    ,to_breed
    ,to_sex
    ,NULL
    ,to_pprt_ver_no
    ,decode(cts_list,'ADD',NULL,to_date_on)
    ,decode(cts_list,'ADD',NULL,to_date_off)
    ,NULL
    ,decode(cts_list,'ADD',NULL,to_death_date)
    ,NULL
    ,NULL
    ,to_dam_etag
    ,decode(cts_list,'ADD',NULL,to_first_calving)
    ,from_animal_seen
    ,from_double_tag
    ,decode(from_cir_code,NULL,'Yes','No')
    ,decode(cts_list,'AL1',decode(from_location,to_location,'Yes'
                                                                  ,'No'),'No')
    ,from_docs_retained
    ,NULL
    ,NULL
    ,decode(from_cir_code,'DB','Yes','ORDB','OR',NULL)
    ,decode(from_cir_code,'ID','Yes','ORID','OR',NULL)
    ,decode(from_cir_code,'LZ','Yes','ORLZ','OR',NULL)
    ,decode(from_cir_code,'OP','Yes','OROP','OR',NULL)
    ,decode(from_cir_code,'FM','Yes','ORFM','OR',NULL)
    ,decode(from_cir_code,'MV','Yes','ORMV','OR',NULL)
    ,decode(from_cir_code,'NF','Yes','ORNF','OR',NULL)
    ,decode(from_cir_code,'NA','Yes','ORNA','OR',NULL)
    ,decode(from_cir_code,'NP','Yes','ORNP','OR',NULL)
    ,decode(substr(from_cir_code,1,3)
                        ,'TG1','Yes'
                        ,'TG2','Yes'
                        ,decode(substr(from_cir_code,3,3)
                               ,'TG1','OR'
                               ,'TG2','OR'
                               ,NULL)
                        )
    ,decode(from_cir_code,'DD','Yes','ORDD','OR',NULL)
    ,decode(from_cir_code,'CD','Yes','ORCD','OR',NULL)
    ,substr(from_anim_comm,1,500)     
    ,decode(to_animal,from_animal,NULL,from_animal)
    ,decode(to_dob,from_dob,NULL,from_dob)
    ,decode(to_breed,from_breed,NULL,from_breed)
    ,decode(to_sex,from_sex,NULL,from_sex)
    ,decode(substr(to_pprt_ver_no,2,2),substr(from_pprt_ver_no,2,2),NULL,from_pprt_ver_no)
    ,decode(to_date_on,from_date_on,NULL,from_date_on)
    ,decode(to_date_off,from_date_off,NULL,from_date_off)
    ,NULL
    ,decode(to_death_date,from_death_date,NULL,from_death_date)
    ,decode(rtrim(to_dam_etag,' '),rtrim(from_dam_etag,' ')
                                                               ,NULL,from_dam_etag)
    ,decode(to_location,from_location,NULL,from_location)
    ,decode(cts_list||from_recs_seen,'AL2Y','Yes','AL3Y','Yes',NULL) second_part
    FROM inspections_all
    ,(
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, t1, t2 ) */
      DISTINCT 'AL1' cts_list, 'AL1-1'                                          query_cts_list
      ,      fromrec.iad_id                                                     from_iad_id
      ,      fromrec.iad_i_id                                                   insp
      ,      torec.iad_eartag_no                                                to_animal
      ,      fromrec.iad_eartag_no                                              from_animal
      ,      torec.iad_location_code                                            to_location
      ,      'S1'                                                               cts_type
      ,      TO_CHAR((torec.iad_dob), 'DD-MON-YYYY')                            to_dob
      ,      fromrec.iad_dob                                                    from_dob
      ,      torec.iad_breed                                                    to_breed
      ,      fromrec.iad_breed                                                  from_breed
      ,      decode(torec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
               ,'B','M','b','M','ST','M','st','M','m','M',torec.iad_sex)        to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)        from_sex
      ,      torec.iad_passport_version                                         to_pprt_ver_no
      ,      fromrec.iad_passport_version                                       from_pprt_ver_no
      ,      torec.iad_dam_ear_tag                                              to_dam_etag
      ,      fromrec.iad_dam_ear_tag                                            from_dam_etag
      ,      torec.iad_date_moved_on                                            to_date_on
      ,      fromrec.iad_date_moved_on                                          from_date_on
      ,      torec.iad_date_moved_off                                           to_date_off
      ,      fromrec.iad_date_moved_off                                         from_date_off
      ,      torec.iad_date_of_death                                            to_death_date
      ,      fromrec.iad_date_of_death                                          from_death_date
      ,      torec.iad_first_calving_date                                       to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No')                     from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No')                             from_double_tag
      ,      decode(cir_code,'MV','No'
                            ,'FM','No'
                            ,'NP','No'
                            ,'DEL','No'
                            ,'OE','No'
                            ,'OR',decode(docs_retained_code,'Y','Yes','No')
                            ,'DB','Yes'
                            ,'ID','Yes'
                            ,'LZ','Yes'
                            ,'OP','Yes'
                            ,'NF','Yes'
                            ,'TG1','Yes'
                            ,'TG2','Yes'
                            ,'TG3','Yes'
                            ,'DD','Yes'
                            ,'CD','Yes'
                            ,'NA','Yes'
               ,decode(fromrec.iad_eartag_no,torec.iad_eartag_no,'No'
                 , decode(fromrec.iad_old_passport_seen,'Y' ,'Yes'
                   ,decode(fromrec.iad_cts_passport_seen,'Y','Yes'
                     ,decode(fromrec.iad_ccd_seen,'Y','Yes'
      ,      decode(fromrec.iad_cor_seen,'Y','Yes','No'))))),'Yes')             from_docs_retained
      ,      decode(nvl(bad_code,'X'),'X',cir_code
                                         ,bad_code||cir_code
                                         )                                      from_cir_code
      ,      fromrec.iad_location_code                                          from_location
      , substr(REPLACE(REPLACE(coma_text,chr(10),' '),'|',' '),1,500)           from_anim_comm
      , NULL                                                                    from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    bcms_cii_livestock torec
      ,    (SELECT 'Y'                                                          docs_retained_code
            ,     cir_i_id                                                      docs_insp
            ,     cir_bov_eartag                                                docs_tag
            ,     cir_location_code                                             docs_loc
            FROM bcms_cattle_insp_results
            WHERE cir_code IN('DB','ID','LZ','OP','NF','TG1','TG2','TG3',
                              'DD','DD1','CD','NA')
            AND nvl(cir_bov_eartag,'X') != 'X')                                 t1
     ,     (SELECT /*+ USE_HASH (c1, c_or) */ 
                   c1.cir_bov_eartag                                            cii_tag
            ,      c1.cir_i_id                                                  cii_insp
            ,      decode(c1.cir_code,'FM1','FM','FM2','FM','DD1','DD',
                          c1.cir_code)                                          cir_code
            ,      c1.cir_location_code                                         cii_loc
            ,      c_or.cir_code                                                bad_code
            FROM bcms_cattle_insp_results c1
            ,    bcms_cattle_insp_results c_or
            WHERE c1.cir_bov_eartag = c_or.cir_bov_eartag(+)
            AND   c1.cir_i_id       = c_or.cir_i_id (+)
            AND   c1.cir_hh_field   = c_or.cir_hh_field(+)
            AND   nvl(c1.cir_location_code, '1234567890123') 
                = nvl(c_or.cir_location_code (+)
                , nvl(c1.cir_location_code, '1234567890123'))
            AND   c1.cir_code NOT IN ('DEL','OE','BP','SP')
            AND   nvl(c1.cir_bov_eartag,'X') != 'X'
            AND   c_or.cir_code(+) ='OR')                                       t2
      WHERE fromrec.iad_direction = 'FROM'
      AND   fromrec.iad_date_of_death IS NULL
      AND   fromrec.iad_date_moved_off IS NULL
      AND   torec.iad_seq_no = fromrec.iad_seq_no
      AND   torec.iad_i_id = fromrec.iad_i_id
      AND   torec.iad_direction = 'TO'
      AND   fromrec.iad_comm = coma_id(+)
      AND   fromrec.iad_eartag_no = docs_tag(+)
      AND   fromrec.iad_i_id = docs_insp(+)
      AND   nvl(fromrec.iad_location_code, '1234567890123') 
          = nvl(docs_loc(+), nvl(fromrec.iad_location_code, '1234567890123'))
      AND   fromrec.iad_eartag_no = cii_tag
      AND   fromrec.iad_i_id = cii_insp
      AND   nvl(fromrec.iad_location_code, '1234567890123') 
          = nvl(cii_loc, nvl(fromrec.iad_location_code, '1234567890123'))
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      AND   bad_code||cir_code != 'OROR'
      UNION ALL
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, t1 ) */ 
             DISTINCT 'AL2' cts_list, 'AL2-1'                                   query_cts_list
      ,      fromrec.iad_id                                                     from_iad_id
      ,      fromrec.iad_i_id                                                   insp
      ,      torec.iad_eartag_no                                                to_animal
      ,      fromrec.iad_eartag_no                                              from_animal
      ,      torec.iad_location_code                                            to_location
      ,      'S2'                                                               cts_type
      ,      TO_CHAR((torec.iad_dob), 'DD-MON-YYYY')                            to_dob
      ,      fromrec.iad_dob                                                    from_dob
      ,      torec.iad_breed                                                    to_breed
      ,      fromrec.iad_breed                                                  from_breed
      ,      decode(torec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
               ,'B','M','b','M','ST','M','st','M','m','M',torec.iad_sex)        to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)        from_sex
      ,      torec.iad_passport_version                                         to_pprt_ver_no
      ,      fromrec.iad_passport_version                                       from_pprt_ver_no
      ,      torec.iad_dam_ear_tag                                              to_dam_etag
      ,      fromrec.iad_dam_ear_tag                                            from_dam_etag
      ,      torec.iad_date_moved_on                                            to_date_on
      ,      fromrec.iad_date_moved_on                                          from_date_on
      ,      torec.iad_date_moved_off                                           to_date_off
      ,      fromrec.iad_date_moved_off                                         from_date_off
      ,      torec.iad_date_of_death                                            to_death_date
      ,      fromrec.iad_date_of_death                                          from_death_date
      ,      torec.iad_first_calving_date                                       to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No')                     from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No')                             from_double_tag
      ,      decode(cir_code,'NP','No'
                ,decode(fromrec.iad_old_passport_seen,'Y','Yes'
                    ,decode(fromrec.iad_cts_passport_seen,'Y','Yes'
                        ,decode(fromrec.iad_cor_seen,'Y','Yes'
              ,decode(fromrec.iad_ccd_seen,'Y','Yes','No')))))                  from_docs_retained
      ,      decode(nvl(bad_code,'X'),'X',cir_code
                                         ,bad_code||cir_code
                                         )                                      from_cir_code
      ,      fromrec.iad_location_code                                          from_location
      ,      substr(REPLACE(REPLACE(coma_text,chr(10),' '),'|',' '),1,500)      from_anim_comm
      ,      fromrec.iad_records_seen                                           from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    bcms_cii_livestock torec
      ,     (SELECT /*+ USE_HASH (c1, c_or) */ 
                    c1.cir_bov_eartag                                           cii_tag
             ,      c1.cir_i_id                                                 cii_insp
             ,      c1.cir_location_code                                        cii_loc
             ,      decode(c1.cir_code,'FM1','FM','FM2','FM','DD1','DD',
                           c1.cir_code)                                         cir_code
             ,      c_or.cir_code                                               bad_code
            FROM bcms_cattle_insp_results c1
            ,    bcms_cattle_insp_results c_or
            WHERE c1.cir_bov_eartag = c_or.cir_bov_eartag(+)
            AND   c1.cir_i_id       = c_or.cir_i_id (+)
            AND   c1.cir_hh_field   = c_or.cir_hh_field(+)
            AND   nvl(c1.cir_location_code, '1234567890123') 
                = nvl(c_or.cir_location_code (+)
                , nvl(c1.cir_location_code, '1234567890123'))
            AND   c1.cir_code NOT IN ('DEL','OE','BP','SP')
            AND   nvl(c1.cir_bov_eartag,'X') != 'X'
            AND   c_or.cir_code(+) ='OR')                                       t1
      WHERE fromrec.iad_direction = 'FROM'
      AND   (fromrec.iad_date_of_death IS NOT NULL
             OR fromrec.iad_date_moved_off IS NOT NULL)
      AND   torec.iad_seq_no = fromrec.iad_seq_no
      AND   torec.iad_i_id = fromrec.iad_i_id
      AND   torec.iad_direction = 'TO'
      AND   fromrec.iad_comm = coma_id(+)
      AND   fromrec.iad_eartag_no = cii_tag
      AND   (fromrec.iad_date_moved_on IS NULL
             OR trunc(fromrec.iad_date_moved_on) = trunc(fromrec.iad_dob))
      AND   fromrec.iad_i_id = cii_insp
      AND   nvl(fromrec.iad_location_code, '1234567890123')
          = nvl(cii_loc, nvl(fromrec.iad_location_code, '1234567890123'))
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      AND   bad_code||cir_code != 'OROR'
      UNION ALL
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, t1 ) */
             DISTINCT 'AL3' cts_list, 'AL3-1' query_cts_list
      ,      fromrec.iad_id from_iad_id
      ,      fromrec.iad_i_id insp
      ,      torec.iad_eartag_no to_animal
      ,      fromrec.iad_eartag_no from_animal
      ,      torec.iad_location_code to_location
      ,      'S2' cts_type
      ,      TO_CHAR((torec.iad_dob), 'DD-MON-YYYY') to_dob
      ,      fromrec.iad_dob from_dob
      ,      torec.iad_breed to_breed
      ,      fromrec.iad_breed from_breed
      ,      decode(torec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
               ,'B','M','b','M','ST','M','st','M','m','M',torec.iad_sex)to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)from_sex
      ,      torec.iad_passport_version to_pprt_ver_no
      ,      fromrec.iad_passport_version from_pprt_ver_no
      ,      torec.iad_dam_ear_tag to_dam_etag
      ,      fromrec.iad_dam_ear_tag from_dam_etag
      ,      torec.iad_date_moved_on to_date_on
      ,      fromrec.iad_date_moved_on from_date_on
      ,      torec.iad_date_moved_off to_date_off
      ,      fromrec.iad_date_moved_off from_date_off
      ,      torec.iad_date_of_death to_death_date
      ,      fromrec.iad_date_of_death from_death_date
      ,      torec.iad_first_calving_date to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No') from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No') from_double_tag
      ,      decode(cir_code,'NP','No'
                ,decode(fromrec.iad_old_passport_seen,'Y','Yes'
                    ,decode(fromrec.iad_cts_passport_seen,'Y','Yes'
                        ,decode(fromrec.iad_cor_seen,'Y','Yes'
              ,decode(fromrec.iad_ccd_seen,'Y','Yes','No')))))from_docs_retained
      ,      decode(nvl(      bad_code,'X'),'X',cir_code
                                         ,bad_code||cir_code
                                         ) from_cir_code
      ,      fromrec.iad_location_code  from_location
      ,      substr(REPLACE(REPLACE(coma_text,chr(10),' '),'|',' '),1,500) from_anim_comm
      ,      NULL from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    bcms_cii_livestock torec
      ,     (SELECT /*+ USE_HASH (c1, c_or) */ c1.cir_bov_eartag cii_tag
            ,      c1.cir_i_id       cii_insp
            ,      c1.cir_location_code cii_loc
            ,      decode(c1.cir_code,'FM1','FM','FM2','FM','DD1','DD',
                          c1.cir_code)   cir_code
            ,      c_or.cir_code bad_code
            FROM bcms_cattle_insp_results c1
            ,    bcms_cattle_insp_results c_or
            WHERE c1.cir_bov_eartag = c_or.cir_bov_eartag(+)
            AND   c1.cir_i_id = c_or.cir_i_id (+)
            AND   c1.cir_hh_field = c_or.cir_hh_field(+)
            AND   nvl(c1.cir_location_code, '1234567890123') 
                = nvl(c_or.cir_location_code (+)
                , nvl(c1.cir_location_code, '1234567890123'))
            AND   c1.cir_code NOT IN ('DEL','OE','BP','SP')
            AND   nvl(c1.cir_bov_eartag,'X') != 'X'
            AND   c_or.cir_code(+) ='OR') t1
      WHERE fromrec.iad_direction = 'FROM'
      AND   torec.iad_direction = 'TO'
      AND   torec.iad_seq_no = fromrec.iad_seq_no
      AND   torec.iad_i_id = fromrec.iad_i_id
      AND   fromrec.iad_eartag_no = cii_tag
      AND   fromrec.iad_i_id = cii_insp
      AND   nvl(fromrec.iad_location_code, '1234567890123') 
          = nvl(cii_loc, nvl(fromrec.iad_location_code, '1234567890123'))
      AND   fromrec.iad_date_moved_on IS NOT NULL
      AND   fromrec.iad_comm = coma_id(+)
      AND   trunc(fromrec.iad_date_moved_on) != trunc(fromrec.iad_dob)
      AND   (fromrec.iad_date_moved_off IS NOT NULL
             OR fromrec.iad_date_of_death IS NOT NULL)
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      AND   bad_code||cir_code != 'OROR'
      UNION ALL
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, inspections_all, t1, t2, t3 , t4 ) */
             DISTINCT 'ADD' cts_list, 'ADD-1' query_cts_list
      ,      fromrec.iad_id from_iad_id
      ,      fromrec.iad_i_id insp
      ,      NULL to_animal
      ,      fromrec.iad_eartag_no from_animal
      ,      NULL to_location
      ,      decode(fromrec.iad_animal_seen,'Y','S1','S2') cts_type
      ,      TO_CHAR((SYSDATE), 'DD-MON-YYYY') to_dob
      ,      fromrec.iad_dob from_dob
      ,      NULL to_breed
      ,      fromrec.iad_breed from_breed
      ,      NULL to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)from_sex
      ,      NULL to_pprt_ver_no
      ,      fromrec.iad_passport_version from_pprt_ver_no
      ,      NULL to_dam_etag
      ,      fromrec.iad_dam_ear_tag from_dam_etag
      ,      SYSDATE to_date_on
      ,      fromrec.iad_date_moved_on from_date_on
      ,      SYSDATE to_date_off
      ,      fromrec.iad_date_moved_off from_date_off
      ,      SYSDATE to_death_date
      ,      fromrec.iad_date_of_death from_death_date
      ,      SYSDATE to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No') from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No') from_double_tag
      ,      decode(cir_code,'MV','No'
                            ,'FM','No'
                            ,'NP','No'
                            ,'DEL','No'
                            ,'OE','No'
                            ,'OR',decode(docs_retained_code,'Y','Yes','No')
                            ,NULL,'No'
                                 ,'Yes') from_docs_retained
            ,      decode(nvl(bad_code,'X'),'X',cir_code
                                               ,bad_code||cir_code
                                               ) from_cir_code
      ,      nvl(fromrec.iad_location_code,loc_location)  from_location
      ,      substr(REPLACE(REPLACE(coma_text,chr(10),' '),'|',' '),1,500)  from_anim_comm
      ,      NULL from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    inspections_all
      ,    (SELECT 'Y' docs_retained_code
            ,     cir_i_id docs_insp
            ,     cir_bov_eartag docs_tag
            ,     cir_location_code docs_loc
            FROM bcms_cattle_insp_results
            ,    bcms_cii_inspection_queue
            WHERE cir_code IN('DB','ID','LZ','OP','NF','TG1','TG2','TG3',
                              'DD','DD1','CD','NA')
            AND  bciq_status = 'P' 
            AND cir_i_id = bciq_i_id                              
            AND nvl(cir_bov_eartag,'X') != 'X') t1
     ,     (SELECT /*+ USE_HASH (c1, c_or) */ c1.cir_bov_eartag cii_tag
            ,      c1.cir_i_id       cii_insp
            ,      c1.cir_location_code cii_loc
            ,      decode(c1.cir_code,'FM1','FM','FM2','FM','DD1','DD',c1.cir_code)   cir_code
            ,      c_or.cir_code bad_code
            FROM bcms_cattle_insp_results c1
            ,    bcms_cattle_insp_results c_or
            ,    bcms_cii_inspection_queue
            WHERE c1.cir_bov_eartag = c_or.cir_bov_eartag(+)
            AND  bciq_status = 'P' 
            AND c1.cir_i_id = bciq_i_id                              
            AND   c1.cir_i_id = c_or.cir_i_id (+)
            AND   c1.cir_hh_field = c_or.cir_hh_field(+)
            AND   nvl(c1.cir_location_code, '1234567890123') = nvl(c_or.cir_location_code (+)
                                                                  , nvl(c1.cir_location_code, '1234567890123'))
            AND   c1.cir_code NOT IN ('DEL','OE','BP','SP')
            AND   nvl(c1.cir_bov_eartag,'X') != 'X'
            AND   c_or.cir_code(+) ='OR') t2
     ,      (SELECT /*+ USE_HASH (REQUESTED_LOCATIONS1, inspections_all)  */
                    rl_location_code loc_location
             ,      i_id    loc_i_id
             FROM requested_locations
             ,    inspections_all
             WHERE rl_location_type = 'Main Location Code'
             AND   rl_i_id = i_id
             AND   i_type = 'LI') t3,
       (SELECT iad_i_id, MIN(iad2.created_date) + (1/1440) create_date_plus
                                   FROM bcms_cii_livestock iad2
                         WHERE iad2.iad_direction = 'FROM' GROUP BY iad_i_id) t4
      WHERE fromrec.iad_direction = 'FROM'
      AND   i_id = loc_i_id
      AND   i_type = 'LI'
      AND   i_id = cii_insp
      AND   fromrec.iad_eartag_no = cii_tag
      AND   fromrec.iad_i_id = cii_insp
      AND   nvl(fromrec.iad_location_code, '1234567890123') = nvl(cii_loc, nvl(fromrec.iad_location_code, '1234567890123'))
      AND   fromrec.iad_i_id = i_id
      AND   fromrec.iad_comm = coma_id(+)
      AND   fromrec.iad_eartag_no = docs_tag(+)
      AND   fromrec.iad_i_id = docs_insp(+)
      AND   nvl(fromrec.iad_location_code, '1234567890123') = nvl(docs_loc(+), nvl(fromrec.iad_location_code, '1234567890123'))
      AND   fromrec.iad_i_id = t4.iad_i_id
      AND   fromrec.created_date > t4.create_date_plus
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      AND   bad_code||cir_code != 'OROR'
      UNION ALL
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, inspections_all ) */
             DISTINCT 'AL1' cts_list, 'AL1-2'                                   query_cts_list
      ,      fromrec.iad_id                                                     from_iad_id
      ,      fromrec.iad_i_id                                                   insp
      ,      torec.iad_eartag_no                                                to_animal
      ,      fromrec.iad_eartag_no                                              from_animal
      ,      torec.iad_location_code                                            to_location
      ,      'S1'                                                               cts_type
      ,      TO_CHAR((torec.iad_dob), 'DD-MON-YYYY')                            to_dob
      ,      fromrec.iad_dob                                                    from_dob
      ,      torec.iad_breed                                                    to_breed
      ,      fromrec.iad_breed                                                  from_breed
      ,      decode(torec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
               ,'B','M','b','M','ST','M','st','M','m','M',torec.iad_sex)        to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)        from_sex
      ,      torec.iad_passport_version                                         to_pprt_ver_no
      ,      fromrec.iad_passport_version                                       from_pprt_ver_no
      ,      torec.iad_dam_ear_tag                                              to_dam_etag
      ,      fromrec.iad_dam_ear_tag                                            from_dam_etag
      ,      torec.iad_date_moved_on                                            to_date_on
      ,      fromrec.iad_date_moved_on                                          from_date_on
      ,      torec.iad_date_moved_off                                           to_date_off
      ,      fromrec.iad_date_moved_off                                         from_date_off
      ,      torec.iad_date_of_death                                            to_death_date
      ,      fromrec.iad_date_of_death                                          from_death_date
      ,      torec.iad_first_calving_date                                       to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No')                     from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No')                             from_double_tag
      ,      decode(fromrec.iad_eartag_no,torec.iad_eartag_no,'No'
              , decode(fromrec.iad_old_passport_seen,'Y' ,'Yes'
                      ,decode(fromrec.iad_cts_passport_seen,'Y','Yes'
                         ,decode(fromrec.iad_ccd_seen,'Y','Yes'
                            ,decode(fromrec.iad_cor_seen,'Y','Yes','No')))))    docs_retained
      ,      NULL
      ,      fromrec.iad_location_code                                          from_location
      ,      substr(REPLACE(coma_text,chr(10),' '),1,500)                       from_anim_comm
      ,      NULL                                                               from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    bcms_cii_livestock torec
      ,    inspections_all
      WHERE fromrec.iad_i_id = i_id
      AND   i_type = 'LI'
      AND   fromrec.iad_direction = 'FROM'
      AND   fromrec.iad_date_of_death IS NULL
      AND   fromrec.iad_date_moved_off IS NULL
      AND   torec.iad_seq_no = fromrec.iad_seq_no
      AND   torec.iad_i_id = fromrec.iad_i_id
      AND   torec.iad_direction = 'TO'
      AND   fromrec.iad_comm = coma_id(+)
--      AND   NOT EXISTS(SELECT 'x'
--                       FROM cattle_insp_results
--                       WHERE cir_i_id = i_id
--                       AND   cir_code NOT IN ('OR','OE','BP','SP') 
--                       AND   cir_bov_eartag = fromrec.iad_eartag_no
--                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
--                             = nvl(fromrec.iad_location_code, '1234567890123')
--                       AND   cir_domain = 'CII IRREGULARITY CODES')
      AND   NOT EXISTS(SELECT 1
                       FROM bcms_cattle_insp_results
                       WHERE cir_i_id = i_id
                       AND   cir_code NOT IN ('OR','OE','BP','SP') 
                       AND   cir_bov_eartag = fromrec.iad_eartag_no
                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
                             = nvl(fromrec.iad_location_code, '1234567890123')
                       AND   cir_domain = 'CII IRREGULARITY CODES')
-------------------------------------------------------------------------------------------------                       AND   cir_iad_id = fromrec.iad_id)
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      UNION ALL
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, inspections_all ) */
             DISTINCT 'AL2' cts_list, 'AL2-2'                                   query_cts_list
      ,      fromrec.iad_id                                                     from_iad_id
      ,      fromrec.iad_i_id                                                   insp
      ,      torec.iad_eartag_no                                                to_animal
      ,      fromrec.iad_eartag_no                                              from_animal
      ,      torec.iad_location_code                                            to_location
      ,      'S2'                                                               cts_type
      ,      TO_CHAR((torec.iad_dob), 'DD-MON-YYYY')                            to_dob
      ,      fromrec.iad_dob                                                    from_dob
      ,      torec.iad_breed                                                    to_breed
      ,      fromrec.iad_breed                                                  from_breed
      ,      decode(torec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
               ,'B','M','b','M','ST','M','st','M','m','M',torec.iad_sex )       to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)        from_sex
      ,      torec.iad_passport_version                                         to_pprt_ver_no
      ,      fromrec.iad_passport_version                                       from_pprt_ver_no
      ,      torec.iad_dam_ear_tag                                              to_dam_etag
      ,      fromrec.iad_dam_ear_tag                                            from_dam_etag
      ,      torec.iad_date_moved_on                                            to_date_on
      ,      fromrec.iad_date_moved_on                                          from_date_on
      ,      torec.iad_date_moved_off                                           to_date_off
      ,      fromrec.iad_date_moved_off                                         from_date_off
      ,      torec.iad_date_of_death                                            to_death_date
      ,      fromrec.iad_date_of_death                                          from_death_date
      ,      torec.iad_first_calving_date                                       to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No')                     from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No')                             from_double_tag
      ,      decode(fromrec.iad_old_passport_seen,'Y','Yes'
                    ,decode(fromrec.iad_cts_passport_seen,'Y','Yes'
                        ,decode(fromrec.iad_cor_seen,'Y','Yes'
                           ,decode(fromrec.iad_ccd_seen,'Y','Yes','No'))))      from_docs_retained
      ,      NULL
      ,      fromrec.iad_location_code                                          from_location
      ,      substr(REPLACE(coma_text,chr(10),' '),1,500)                       from_anim_comm
      ,      fromrec.iad_records_seen                                           from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    bcms_cii_livestock torec
      ,    inspections_all
      WHERE i_type = 'LI'
      AND   fromrec.iad_i_id = i_id
      AND   torec.iad_i_id = i_id
      AND   fromrec.iad_direction = 'FROM'
      AND   (fromrec.iad_date_of_death IS NOT NULL
             OR fromrec.iad_date_moved_off IS NOT NULL)
      AND   torec.iad_seq_no = fromrec.iad_seq_no
      AND   torec.iad_i_id = fromrec.iad_i_id
      AND   torec.iad_direction = 'TO'
      AND   fromrec.iad_comm = coma_id(+)
      AND   (         fromrec.iad_date_moved_on IS NULL
             OR trunc(fromrec.iad_date_moved_on) = trunc(fromrec.iad_dob))
--      AND   NOT EXISTS(SELECT 'x'
--                       FROM cattle_insp_results
--                       WHERE cir_i_id = i_id
--                       AND   cir_code NOT IN ('OR','OE','BP','SP')
--                       AND   cir_bov_eartag = fromrec.iad_eartag_no
--                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
--                             = nvl(fromrec.iad_location_code, '1234567890123')
--                       AND   cir_domain = 'CII IRREGULARITY CODES')
      AND   NOT EXISTS(SELECT 1
                       FROM bcms_cattle_insp_results
                       WHERE cir_i_id = i_id
                       AND   cir_code NOT IN ('OR','OE','BP','SP')
                       AND   cir_bov_eartag = fromrec.iad_eartag_no
                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
                             = nvl(fromrec.iad_location_code, '1234567890123')
                       AND   cir_domain = 'CII IRREGULARITY CODES')
---------------------------------------------------------------------------------------                       AND   cir_iad_id = fromrec.iad_id)
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
 UNION ALL
      SELECT /*+ USE_HASH(fromrec, torec, COMMENTS_ALL, inspections_all ) */
             DISTINCT 'AL3' cts_list, 'AL3-2'                                   query_cts_list
      ,      fromrec.iad_id                                                     from_iad_id
      ,      fromrec.iad_i_id                                                   insp
      ,      torec.iad_eartag_no                                                to_animal
      ,      fromrec.iad_eartag_no                                              from_animal
      ,      torec.iad_location_code                                            to_location
      ,      'S2'                                                               cts_type
      ,      TO_CHAR((torec.iad_dob), 'DD-MON-YYYY')                            to_dob
      ,      fromrec.iad_dob                                                    from_dob
      ,      torec.iad_breed                                                    to_breed
      ,      fromrec.iad_breed                                                  from_breed
      ,      decode(torec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
               ,'B','M','b','M','ST','M','st','M','m','M',torec.iad_sex)        to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)        from_sex
      ,      torec.iad_passport_version                                         to_pprt_ver_no
      ,      fromrec.iad_passport_version                                       from_pprt_ver_no
      ,      torec.iad_dam_ear_tag                                              to_dam_etag
      ,      fromrec.iad_dam_ear_tag                                            from_dam_etag
      ,      torec.iad_date_moved_on                                            to_date_on
      ,      fromrec.iad_date_moved_on                                          from_date_on
      ,      torec.iad_date_moved_off                                           to_date_off
      ,      fromrec.iad_date_moved_off                                         from_date_off
      ,      torec.iad_date_of_death                                            to_death_date
      ,      fromrec.iad_date_of_death                                          from_death_date
      ,      torec.iad_first_calving_date                                       to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No')                     from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No')                             from_double_tag
      ,      decode(fromrec.iad_old_passport_seen,'Y','Yes'
                    ,decode(fromrec.iad_cts_passport_seen,'Y','Yes'
                        ,decode(fromrec.iad_cor_seen,'Y','Yes'
                            ,decode(fromrec.iad_ccd_seen,'Y','Yes','No'))))     from_docs_retained
      ,      NULL
      ,      fromrec.iad_location_code                                          from_location
      ,      substr(REPLACE(coma_text,chr(10),' '),1,500)                       from_anim_comm
      ,      NULL                                                               from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    bcms_cii_livestock torec
      ,    inspections_all
   WHERE i_type = 'LI'
      AND   fromrec.iad_i_id = i_id
      AND   torec.iad_i_id = i_id
      AND   fromrec.iad_direction = 'FROM'
      AND   torec.iad_direction = 'TO'
      AND   torec.iad_seq_no = fromrec.iad_seq_no
      AND   torec.iad_i_id = fromrec.iad_i_id
      AND   fromrec.iad_date_moved_on IS NOT NULL
      AND   fromrec.iad_comm = coma_id(+)
      AND   (fromrec.iad_date_moved_off IS NOT NULL
             OR fromrec.iad_date_of_death IS NOT NULL)
      AND   trunc(fromrec.iad_date_moved_on) != trunc(fromrec.iad_dob)
--      AND   NOT EXISTS(SELECT 'x'
--                       FROM cattle_insp_results
--                       WHERE cir_i_id = i_id
--                       AND   cir_code NOT IN ('OR','OE','BP','SP')
--                       AND   cir_bov_eartag = fromrec.iad_eartag_no
--                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
--                                                  = nvl(fromrec.iad_location_code, '1234567890123')
--                       AND   cir_domain = 'CII IRREGULARITY CODES')
      AND   NOT EXISTS(SELECT 1
                       FROM bcms_cattle_insp_results
                       WHERE cir_i_id = i_id
                       AND   cir_code NOT IN ('OR','OE','BP','SP')
                       AND   cir_bov_eartag = fromrec.iad_eartag_no
                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
                                                  = nvl(fromrec.iad_location_code, '1234567890123')
                       AND   cir_domain = 'CII IRREGULARITY CODES')
---------------------------------------------------------------------------------------------                       AND   cir_iad_id = fromrec.iad_id)
      AND   NOT EXISTS (SELECT 1
                        FROM bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      UNION ALL
      SELECT /*+ USE_HASH(fromrec, COMMENTS_ALL, inspections_all, t1 ) */
             DISTINCT 'ADD' cts_list, 'ADD-2'                                   query_cts_list
      ,      fromrec.iad_id                                                     from_iad_id
      ,      fromrec.iad_i_id                                                   insp
      ,      NULL                                                               to_animal
      ,      fromrec.iad_eartag_no                                              from_animal
      ,      NULL                                                               to_location
      ,      decode(fromrec.iad_animal_seen,'Y','S1','S2')                      cts_type
      ,      TO_CHAR((SYSDATE), 'DD-MON-YYYY')                                  to_dob
      ,      fromrec.iad_dob                                                    from_dob
      ,      NULL                                                               to_breed
      ,      fromrec.iad_breed                                                  from_breed
      ,      NULL                                                               to_sex
      ,      decode(fromrec.iad_sex,'c','F','C','F','h','F','H','F','f','F'
             ,'B','M','b','M','ST','M','st','M','m','M',fromrec.iad_sex)        from_sex
      ,      NULL                                                               to_pprt_ver_no
      ,      fromrec.iad_passport_version                                       from_pprt_ver_no
      ,      NULL                                                               to_dam_etag
      ,      fromrec.iad_dam_ear_tag                                            from_dam_etag
      ,      SYSDATE                                                            to_date_on
      ,      fromrec.iad_date_moved_on                                          from_date_on
      ,      SYSDATE                                                            to_date_off
      ,      fromrec.iad_date_moved_off                                         from_date_off
      ,      SYSDATE                                                            to_death_date
      ,      fromrec.iad_date_of_death                                          from_death_date
      ,      SYSDATE                                                            to_first_calving
      ,      decode(fromrec.iad_animal_seen,'Y','Yes','No')                     from_animal_seen
      ,      decode(fromrec.iad_dts,'Y','Yes','No')                             from_double_tag
      ,      'No'                                                               from_docs_retained
      ,      NULL
      ,      nvl(fromrec.iad_location_code,loc_location)                        from_location
      ,      substr(REPLACE(coma_text,chr(10),' '),1,500)                       from_anim_comm
      ,      NULL                                                               from_recs_seen
      FROM comments_all
      ,    bcms_cii_livestock fromrec
      ,    inspections_all
      ,      (SELECT /*+ USE_HASH(REQUESTED_LOCATIONS1, inspections_all) */
                rl_location_code loc_location
              , i_id             loc_i_id
              FROM requested_locations
              ,    inspections_all
              WHERE rl_location_type = 'Main Location Code'
              AND   rl_i_id = i_id
              AND   i_type = 'LI') t1
      WHERE i_type = 'LI'
      AND   i_id = loc_i_id
      AND   fromrec.iad_i_id = i_id
      AND   fromrec.iad_direction = 'FROM'
      AND   fromrec.iad_comm = coma_id(+)
      AND   fromrec.created_date >(SELECT MIN(iad2.created_date) + (1/1440)
                                   FROM bcms_cii_livestock iad2
                                   WHERE iad2.iad_i_id = i_id
                                   AND iad2.iad_direction = 'FROM')
--      AND   NOT EXISTS(SELECT 'x'
--                       FROM cattle_insp_results
--                       WHERE cir_i_id = i_id
--                       AND   cir_code NOT IN ('OR','OE','BP','SP')
--                       AND   cir_bov_eartag = fromrec.iad_eartag_no
--                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
--                                                  = nvl(fromrec.iad_location_code, '1234567890123')
--                       AND   cir_domain = 'CII IRREGULARITY CODES')
      AND   NOT EXISTS(SELECT 1
                       FROM  bcms_cattle_insp_results
                       WHERE cir_i_id = i_id
                       AND   cir_code NOT IN ('OR','OE','BP','SP')
                       AND   cir_bov_eartag = fromrec.iad_eartag_no
                       AND   nvl(cir_location_code, nvl(fromrec.iad_location_code, '1234567890123'))
                                                  = nvl(fromrec.iad_location_code, '1234567890123')
                       AND   cir_domain = 'CII IRREGULARITY CODES')
-------------------------------------------------------------------------                       AND   cir_iad_id = fromrec.iad_id)
      AND   NOT EXISTS (SELECT 1
                        FROM  bcms_cii_results
                        WHERE bcr_i_id = fromrec.iad_i_id
                        AND   bcr_file_type = 'A')
      ) t2
    WHERE i_id = insp
    AND   i_id IN (SELECT bciq_i_id FROM bcms_cii_inspection_queue WHERE bciq_status = 'P');
--                  (SELECT i_id 
--                   FROM   bcms_cii_return_inspect_fields);
                      
    INSERT INTO bcms_cii_return_animals
     (bcra_i_id, 
      bcra_location_code, 
      bcra_eartag_no,
      bcra_line_no,
      bcra_cts_list,
      bcra_iad_id,
      bcra_animal_detail)
    SELECT 
    bcraf_i_id, 
    bcraf_location_code, 
    bcraf_eartag_no,
    bcraf_line_no,
    bcraf_cts_list,
    bcraf_iad_id,
     ani1 || '|' ||
     ani2 || '|' ||
     ani3 || '|' ||
     ani4 || '|' ||
     ani5 || '|' ||
     ani6 || '|' ||
     ani7 || '|' ||
     ani8 || '|' ||
     ani9 || '|' ||
    ani10 || '|' ||
    ani11 || '|' ||
    ani12 || '|' ||
    ani13 || '|' ||
    ani14 || '|' ||
    ani15 || '|' ||
    ani16 || '|' ||
    ani17 || '|' ||
    ani18 || '|' ||
    ani19 || '|' ||
    ani20 || '|' ||
    ani21 || '|' ||
    ani22 || '|' ||
    ani23 || '|' ||
    ani24 || '|' ||
    ani25 || '|' ||
    ani26 || '|' ||
    ani27 || '|' ||
    ani28 || '|' ||
    ani29 || '|' ||
    ani30 || '|' ||
    ani31 || '|' ||
    ani32 || '|' ||
    ani33 || '|' ||
    ani34 || '|' ||
    ani35 || '|' ||
    ani36 || '|' ||
    ani37 || '|' ||
    ani38 || '|' ||
    ani39 || '|' ||
    ani40 || '|' ||
    ani41 || '|' ||
    ani42 || '|' ||
    ani43 || '|' ||
    ani44 || '|' ||
    ani45 || '|' ||
    ani46 || '|' ||
    ani47 || '|' ||
    ani48 || '|' ||
    ani49 || '|' ||
    ani50 || '|' ||
    ani51 || '|' ||
    ani52 || '|' ||
    ani53 || '|' ||
    ani54 || '|' ||
    ani55
    FROM bcms_cii_return_animal_fields;
    
    lv_end_time := SYSDATE;

    pv_err_step_no := 5010;
    p_log_message('p_bcms_cii_return_animals FINISHED succcessfully.'  || LTRIM(TO_CHAR(SQL%ROWCOUNT, '999,999,999,999')) || ' records inserted into bcms_cii_return_animals table. ' ||
                  ' [Elapsed Time '  || LTRIM(TO_CHAR(((lv_end_time  - lv_start_time)*24*60*60), '999,999,999,999')) || ' seconds]');
    p_rtc := 0;
    
  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_return_animals'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_return_animals;

  PROCEDURE p_bcms_cii_return_create_clob ( p_file_ref    IN  VARCHAR2
                                           ,p_raise_error IN  VARCHAR2
                                           ,p_rtc         OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_RETURN_DATA table with CLOB  
    -- *                   data for the current run. One record with inspection CLOB,
    -- *                   one record with holding CLOB and one record with animal CLOB.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************
                                
      clobtext            CLOB; 
      lv_raise_error      VARCHAR2(1) := p_raise_error;
      lv_rtc              NUMBER := 0;
    
      CURSOR c_get_inspection_data IS
      SELECT bcri_inspection_detail
      FROM bcms_cii_return_inspections
      ORDER BY bcri_line_no;
      
      CURSOR c_get_holding_data IS
      SELECT bcrh_holding_detail
      FROM bcms_cii_return_holdings
      ORDER BY bcrh_line_no;
      
      CURSOR c_get_animal_data IS
      SELECT bcra_animal_detail
      FROM bcms_cii_return_animals
      ORDER BY bcra_line_no;
      
    BEGIN

      p_rtc := -1;
      pv_err_step_no := 1005;
   
      clobtext := NULL;
      clobtext := '_'; -- need to initialize it.      

      FOR inspection_data IN c_get_inspection_data LOOP

       DBMS_LOB.APPEND (clobtext, inspection_data.bcri_inspection_detail || CHR(13) || CHR(10));

      END LOOP;
      
      pv_err_step_no := 1010;
      
      -- Remove the leading chacter used to initialize the clob.
      clobtext := SUBSTR(clobtext, 2);
   
      INSERT INTO bcms_cii_return_data 
      (	bcrd_id,
        bcrd_file_type, 
        bcrd_file_ref,
        bcrd_file_name,
        bcrd_data )
      VALUES
      ( bcms_cii_return_data_seq.NEXTVAL,
       'I',
       p_file_ref,
       'SIACS' || TO_CHAR (SYSDATE, 'YYYYDDD') || 'S' || p_file_ref || 'ins' || '.txt',
       clobtext
      );

      p_log_message ('Finished creating Inspections clob: Length = ' || LTRIM(TO_CHAR(LENGTH(clobtext), '999,999,999,999')) || ' Bytes');

      pv_err_step_no := 1015;

      clobtext := NULL;
      clobtext := '_'; -- need to initialize it.      
      pv_err_step_no := 1016;
           
      FOR holding_data IN c_get_holding_data LOOP

       DBMS_LOB.APPEND (clobtext, holding_data.bcrh_holding_detail || CHR(13) || CHR(10));
     
      END LOOP;

      -- Remove the leading chacter used to initialize the clob.
      clobtext := SUBSTR(clobtext, 2);
  
      pv_err_step_no := 1020;
         
      INSERT INTO bcms_cii_return_data 
      (	bcrd_id,
        bcrd_file_type, 
        bcrd_file_ref,
        bcrd_file_name,
        bcrd_data )
      VALUES
      ( bcms_cii_return_data_seq.NEXTVAL,
       'H',
       p_file_ref,
       'SIACS' || TO_CHAR (SYSDATE, 'YYYYDDD') || 'S' || p_file_ref || 'hol' || '.txt',
       clobtext
      );

      p_log_message ('Finished creating Holdings clob: Length = ' || LTRIM(TO_CHAR(LENGTH(clobtext), '999,999,999,999')) || ' Bytes');

      pv_err_step_no := 1025;
      clobtext := ''; 
      pv_err_step_no := 1026;

      p_log_message ('Build animal clob STARTED');
 
      clobtext := NULL;
      clobtext := '_'; -- need to initialize it.      
  
      FOR animal_data IN c_get_animal_data LOOP

        DBMS_LOB.APPEND (clobtext, animal_data.bcra_animal_detail || CHR(13) || CHR(10));
    
      END LOOP;

      -- Remove the leading chacter used to initialize the clob.
      clobtext := SUBSTR(clobtext, 2);
 
      p_log_message ('Build animal clob COMPLETED');
      p_log_message ('INSERT clob for animals INTO bcms_cii_return_data STARTED');
     
      pv_err_step_no := 1030;
   
      INSERT INTO bcms_cii_return_data 
      (	bcrd_id,
        bcrd_file_type, 
        bcrd_file_ref,
        bcrd_file_name,
        bcrd_data )
      VALUES
      ( bcms_cii_return_data_seq.NEXTVAL,
       'A',
       p_file_ref,
       'SIACS' || TO_CHAR (SYSDATE, 'YYYYDDD') || 'S' || p_file_ref || 'ani' || '.txt',
       clobtext
      );

      p_log_message ('INSERT clob for animals INTO bcms_cii_return_data COMPLETED');

      pv_err_step_no := 1035;
           
      p_log_message ('Finished creating Animals clob: Length = ' || LTRIM(TO_CHAR(LENGTH(clobtext), '999,999,999,999')) || ' Bytes'); 

      p_rtc := 0;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_return_create_clob'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_return_create_clob;

  PROCEDURE p_bcms_cii_return_pop_results ( p_file_ref    IN  VARCHAR2
                                           ,p_raise_error IN  VARCHAR2
                                           ,p_rtc         OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Populate the BCMS_CII_RESULTS table with records to indicate the contents  
    -- *                   of the current run. One record per inspection with file type 'I',
    -- *                   one record per inspection with file type 'H', and one record per inspection 
    -- *                   with file type 'A',
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************
                                

      kount PLS_INTEGER := 0;                            

      lv_file_ref         bcms_cii_results.bcr_file_ref%TYPE := p_file_ref;    
      lv_raise_error      VARCHAR2(1) := p_raise_error;
      lv_rtc              NUMBER := 0;

      CURSOR c_get_inspections IS
       SELECT bciq_i_id
       FROM   bcms_cii_inspection_queue
       WHERE  bciq_status = 'P'
       ORDER BY bciq_i_id;
       
  BEGIN

      p_rtc := -1;
      pv_err_step_no := 2000;
    
      FOR inspections IN c_get_inspections LOOP
      
        kount := kount + 1;
        p_log_message('Inspection ' || inspections.bciq_i_id);
    
        INSERT INTO bcms_cii_results
        (bcr_i_id
        ,bcr_run_date
        ,bcr_file_type
        ,bcr_file_ref)
        VALUES
        ( inspections.bciq_i_id
         ,SYSDATE
         ,'I'
         , lv_file_ref );
        
        INSERT INTO bcms_cii_results
        (bcr_i_id
        ,bcr_run_date
        ,bcr_file_type
        ,bcr_file_ref)
        VALUES
        ( inspections.bciq_i_id
         ,SYSDATE
         ,'H'
         , lv_file_ref );
            
        INSERT INTO bcms_cii_results
        (bcr_i_id
        ,bcr_run_date
        ,bcr_file_type
        ,bcr_file_ref)
        SELECT
         inspections.bciq_i_id
         ,SYSDATE
         ,'A'
         , lv_file_ref
         FROM dual
         WHERE EXISTS
            ( SELECT 1 
              FROM bcms_cii_livestock
              WHERE iad_i_id = inspections.bciq_i_id);
                 
      END LOOP;
  
      pv_err_step_no := 2010;
           
      p_log_message('Finished populating BCMS_CII_RESULTS table with I, H and A records for ' || kount || ' inspections.'); 
      
      -- Now mark the selected batch as complete
        UPDATE bcms_cii_inspection_queue 
        SET    bciq_file_ref = lv_file_ref,
               bciq_status = 'C'
        WHERE  bciq_status = 'P'
        AND bciq_i_id IN 
            (SELECT TO_NUMBER(SUBSTR(ins5, 2))
             FROM bcms_cii_return_inspect_fields);

      p_log_message       ('Updated bcms_cii_inspection_queue table to show inspections which have now been processed'); 

      pv_err_step_no := 2020;

      p_rtc := 0;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_return_pop_results'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_bcms_cii_return_pop_results;
    
  PROCEDURE p_bcms_cii_returns ( p_inspection_count OUT INTEGER
                                ,p_raise_error      IN  VARCHAR2
                                ,p_rtc              OUT INTEGER) IS

    -- *********************************************************************************************
    -- * Purpose         : Main controlling procedure and entry point to the procedures for extracting 
    -- *                   data to be sent to BCMS for all inspection completed in the past week.
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   16-MAY-2018  Initial version
    -- *********************************************************************************************

    CURSOR c_get_batch_to_requeue IS
    SELECT bciq_file_ref
    FROM  bcms_cii_inspection_queue
    WHERE bciq_status = 'P';
                                
    lv_inspection_count       PLS_INTEGER := -1;
    lv_inspections_processed  PLS_INTEGER := -1;
    lv_raise_error            VARCHAR2(1) := p_raise_error;
    lv_rtc                    NUMBER := 0;
    lv_file_ref               bcms_cii_results.bcr_file_ref%TYPE;
    lv_batch_to_requeue       bcms_cii_results.bcr_file_ref%TYPE;
    lv_extract_start_time     DATE := SYSDATE;
    lv_extract_end_time       DATE;

  BEGIN
  
    p_rtc := -1;
    pv_err_step_no := 1;
    
    p_log_message ('---------------------------------------------------------------------------------------------------------------------------------------------------------------');
    p_log_message ('EXTRACT STARTING');
    
 
    pv_err_step_no := 2;

    -- Check for any batches that failed on the last run and if found, reset them for re-processing.
    -- This might have been due to a FUSE failure for instance.
    OPEN  c_get_batch_to_requeue;
    FETCH c_get_batch_to_requeue INTO lv_batch_to_requeue;
    CLOSE c_get_batch_to_requeue;

    pv_err_step_no := 3;
    
    IF lv_batch_to_requeue IS NOT NULL THEN

      p_log_message       ('Detected Failed BATCH ' || lv_batch_to_requeue || ' - attempting to re-queue.');

      p_bcms_cii_reset_batch ( p_file_ref      => lv_batch_to_requeue
                              ,p_raise_error   => lv_raise_error
                              ,p_rtc           => lv_rtc);

    END IF;

    pv_err_step_no := 4;

    OPEN  c_get_inspection_count;
    FETCH c_get_inspection_count INTO lv_inspection_count;
    CLOSE c_get_inspection_count;

    pv_err_step_no := 15;

    p_log_message       (lv_inspection_count || ' candidate inspections in queue waiting to be processed.');

    -- Only continue if there are inspections to be processed
    IF NVL(lv_inspection_count, -1) > 0 THEN
    
      pv_err_step_no := 22;
  
      p_bcms_cii_delete_old_tmp_data( p_raise_error   => lv_raise_error
                             ,p_rtc           => lv_rtc);
   
      p_log_message ('p_bcms_cii_delete_old_tmp_data COMPLETED');

      pv_err_step_no := 22;
  
      OPEN  c_get_next_file_ref;
      FETCH c_get_next_file_ref INTO lv_file_ref;
      CLOSE c_get_next_file_ref;

      p_log_message ('New Batch Number ' || lv_file_ref ||' ASSIGNED');

      pv_err_step_no := 30;
 
      p_select_next_batch ( p_file_ref     => lv_file_ref
                           ,p_raise_error  => lv_raise_error
                           ,p_rtc          => lv_rtc );

      pv_err_step_no := 25;
  
 
      p_bcms_cii_return_inspections( p_file_ref    => lv_file_ref
                                    ,p_raise_error => lv_raise_error
                                    ,p_rtc         => lv_rtc);
      
      pv_err_step_no := 40;
      p_log_message ('p_bcms_cii_return_inspections COMPLETED');

      p_bcms_cii_pop_livestock  ( p_raise_error   => lv_raise_error
                                 ,p_rtc           => lv_rtc);
      
      pv_err_step_no := 30;
      p_log_message ('p_bcms_cii_pop_livestock COMPLETED');
  
      p_bcms_cii_pop_cattle_insp_res  ( p_raise_error   => lv_raise_error
                                       ,p_rtc           => lv_rtc);
      
      pv_err_step_no := 35;
      p_log_message ('p_bcms_cii_pop_cattle_insp_res COMPLETED');
    
      p_bcms_cii_return_holdings( p_file_ref    => lv_file_ref
                                 ,p_raise_error => lv_raise_error
                                 ,p_rtc         => lv_rtc);
      
      pv_err_step_no := 50;
      p_log_message ('p_bcms_cii_return_holdings COMPLETED');
  
      p_bcms_cii_return_animals( p_file_ref    => lv_file_ref
                                ,p_raise_error => lv_raise_error
                                ,p_rtc         => lv_rtc);

      pv_err_step_no := 55;
      p_log_message ('p_bcms_cii_return_animals COMPLETED');
  
      p_bcms_cii_return_create_clob ( p_file_ref    => lv_file_ref
                                     ,p_raise_error => lv_raise_error
                                     ,p_rtc         => lv_rtc);
      p_log_message ('p_bcms_cii_return_create_clob COMPLETED');

--    Commented out because this procedure will now be called by FUSE once CLOB files have been emailed to CTS.
--      p_bcms_cii_return_pop_results ( p_file_ref    => lv_file_ref
--                                     ,p_raise_error => lv_raise_error
--                                     ,p_rtc         => lv_rtc);
--      p_log_message ('p_bcms_cii_return_pop_results COMPLETED');
 
       -- Count the number of inspections which have been processed in the current batch.                                
      OPEN  c_get_inspections_processed; 
      FETCH c_get_inspections_processed INTO lv_inspections_processed;
      CLOSE c_get_inspections_processed; 
 
      p_log_message       ('p_bcms_cii_returns FINISHED successfully. Batch Number is ' || lv_file_ref ||' and ' || lv_inspections_processed || ' inspections were processed.');

    ELSE
    
       --  No inspections to be processed
      p_log_message       ('p_bcms_cii_returns COMPLETED but there were no inspections to be processed');
      
      lv_inspections_processed := 0;

    END IF;
    
    pv_err_step_no := 60;
    lv_extract_end_time := SYSDATE;

    p_log_message ('EXTRACT COMPLETED' ||
                   ' [Total time taken '  || LTRIM(TO_CHAR(((lv_extract_end_time  - lv_extract_start_time)*24*60*60), '999,999,999,999')) || ' seconds]');

    p_inspection_count := lv_inspections_processed;
    p_rtc := 0;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_bcms_cii_returns'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        p_log_message       ('*** EXTRACT FAILED **** Full data ROLLBACK performed due to fatal error - Check STD_ERR_LOG !!');
        RAISE;
      END IF;

  END p_bcms_cii_returns;

  PROCEDURE p_update_max_batch_size ( p_batch_size       IN  INTEGER
                                     ,p_raise_error      IN  VARCHAR2
                                     ,p_rtc              OUT INTEGER) IS
    -- *********************************************************************************************
    -- * Purpose         : Update the maximum number of inspections to be included in BCMS Return Batch
    -- *
    -- * Who                Ver   When        What
    -- * ----------------   ----  ----------  ---------------------------------------------------------
    -- * Iain High    1.0   15-MAR-2019  Initial version
    -- *********************************************************************************************
                                     
    lv_raise_error   VARCHAR2(1) := p_raise_error;
    lv_rtc           NUMBER := 0;

  BEGIN
  
    pv_err_step_no := 555;

    IF p_batch_size BETWEEN 1 AND 5000 THEN

      pv_err_step_no := 560;
    
      UPDATE ref_codes_all 
       SET rc_value = p_batch_size
      WHERE rc_domain = 'BCMS RETURNS MAX BATCH SIZE';

      pv_err_step_no := 565;

      p_log_message       ('BCMS Return Batch maximum size set to ' || TO_CHAR(p_batch_size));

      pv_err_step_no := 570;
      p_rtc := 0;
      
    ELSE

      p_log_message       ('Attempt to set BCMS Return Batch maximum size FAILED');

      pv_err_step_no := 575;
      p_rtc := 1;
           
    END IF;

  EXCEPTION

    WHEN OTHERS THEN
      lv_rtc := ai_err_log_pkg.write_err_log(pv_err_system
                                            ,sysdate
                                            ,pv_err_module || ' - p_update_max_batch_size'
                                            ,'F'
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,pv_err_step_no
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL
                                            ,NULL);
      IF lv_raise_error = 'Y' THEN
        p_rtc := -1;
        ROLLBACK;
        RAISE;
      END IF;

  END p_update_max_batch_size;

END ai_bcms_cii_returns_pkg;
/
