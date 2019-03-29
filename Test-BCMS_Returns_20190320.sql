-- 20190329 - Now using the D:\sql\Return-Data-To-CTS-GIT folder - 5th Commit
/*
NEXT RELEASE

BCMS_Returns-CREATE-TABLES-RELEASE2-V3.sql
SPEC
BODY
*/

/* REPORT NAMES
ReturnDatatoCTSDataModelReport
Return Data to CTS Data Model Report
*/

-- SIACS CODES
/*
DD	CII IRREGULARITY CODES	Dead animal/passport not returned for dead animal
DEL	CII IRREGULARITY CODES	Animal not part of herd
ID	CII IRREGULARITY CODES	Dam ID error (other than space or leading zero issues)
ID	CII IRREGULARITY CODES	Dam ID error with potential DBES impact
LZ	CII IRREGULARITY CODES	Less serious Dam ID error (eg Leading zeros)
MV	CII IRREGULARITY CODES	Movement details not recorded/incorrectly recorded
NA	CII IRREGULARITY CODES	Passport present without animal
NP	CII IRREGULARITY CODES	Animal without passport
OE	CII IRREGULARITY CODES	Obvious Error
OP	CII IRREGULARITY CODES	Other incorrect passport details (sex, breed, ET)
OP	CII IRREGULARITY CODES	Incorrect breed or sex
OR	CII IRREGULARITY CODES	Override
TG1	CII IRREGULARITY CODES	Not identifiable
TG1	CII IRREGULARITY CODES	Not identifiable
TG2	CII IRREGULARITY CODES	Incorrectly Tagged
TG2	CII IRREGULARITY CODES	Incorrectly Tagged
TG3	CII IRREGULARITY CODES	Missing tag but identifiable
*/

SET VERIFY OFF
UNDEFINE p_i_id;
DEFINE p_i_id=607676;
DEFINE p_i_id=593176;
DEFINE p_i_id=NULL;
DEFINE p_i_id=551434;

-- Ex SIT Case.
DEFINE p_i_id=60003526;

--START D:\sql\Return-Data-To-CTS\V312__REL2-V6-ai_bcms_cii_returns_pkg_spec.sql;
--START D:\sql\Return-Data-To-CTS\V313__REL2-V20-ai_bcms_cii_returns_pkg_body.sql;
START H:\AnimalInspections\sql\Return-Data-To-CTS\V312__REL2-V6-ai_bcms_cii_returns_pkg_spec.sql;
START H:\AnimalInspections\sql\Return-Data-To-CTS\V313__REL2-V20-ai_bcms_cii_returns_pkg_body.sql;

SHOW ERROR

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--PROCESS ALL INSPECTIONS IN THE NEXT BATCH
SET SERVEROUTPUT ON SIZE 500000
DECLARE

  CURSOR c_get_batch_no IS
  SELECT MAX(bcrd_file_ref) 
  FROM bcms_cii_return_data;

  lv_inspection_count PLS_INTEGER := -1;
  lv_file_ref         bcms_cii_return_data.bcrd_file_ref%TYPE := '-1';
  lv_raise_error      VARCHAR2(1) := 'Y';
  lv_rtc              PLS_INTEGER := -1;
BEGIN  

  ai_bcms_cii_returns_pkg.p_bcms_cii_returns( p_inspection_count   => lv_inspection_count
                                             ,p_raise_error        => lv_raise_error
                                             ,p_rtc                => lv_rtc);

  --lv_rtc := -1;  -- Un-comment this line to simulate FUSE failing.

  IF lv_rtc = 0 THEN
    -- This is the bit that is done by FUSE :-
    
    OPEN  c_get_batch_no;
    FETCH c_get_batch_no INTO lv_file_ref;
    CLOSE c_get_batch_no;

    IF TO_NUMBER(lv_file_ref) > -1 THEN

      DBMS_OUTPUT.PUT_LINE('FUSE Succeeded. File Ref  = ' || lv_file_ref);
    
      ai_bcms_cii_returns_pkg.p_bcms_cii_return_pop_results ( p_file_ref    => lv_file_ref
                                                             ,p_raise_error => lv_raise_error
                                                             ,p_rtc         => lv_rtc);
    ELSE
      lv_rtc := -1;
    END IF;
    
  END IF;
  
  IF    NVL(lv_rtc, -1) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!. lv_rtc = ' || lv_rtc);
  ELSE
    DBMS_OUTPUT.PUT_LINE(lv_inspection_count || ' inspections processed.');
  END IF;
  
END;
/
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- ROLLBACK SPECIFIED BATCH
SET SERVEROUTPUT ON SIZE 500000
DECLARE
--  lv_file_ref         bcms_cii_results.bcr_file_ref%TYPE := '8409';
  lv_file_ref         bcms_cii_results.bcr_file_ref%TYPE := '9357';
--  lv_file_ref         bcms_cii_results.bcr_file_ref%TYPE := '1';
  lv_raise_error      VARCHAR2(1) := 'Y';
  lv_rtc              PLS_INTEGER := -1;
BEGIN  

  ai_bcms_cii_returns_pkg.p_bcms_cii_rollback_batch( p_file_ref           => lv_file_ref
                                                    ,p_raise_error        => lv_raise_error
                                                    ,p_rtc                => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -1) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('ROLLED BACK LAST BATCH ' || lv_file_ref);
  END IF;
  
END;

SELECT * from INSPECTIONS_ALL WHERE I_ID IN (
60005670,
60005675,
60005696,
60005701,
60005702,
60005727,
60005729,
60005742,
60005750);

-- ADD SPECIFIED INSPECTION TO NEXT BATCH
SET SERVEROUTPUT ON SIZE 500000
DECLARE
--  lv_inspection_id    inspections_all.i_id%TYPE := 592900; -- Should fail - no inspection pack
--  lv_inspection_id    inspections_all.i_id%TYPE := 596980;
--  lv_inspection_id    inspections_all.i_id%TYPE := 609621; --already sent to BCMS
--  lv_inspection_id    inspections_all.i_id%TYPE := 609273;
--  lv_inspection_id    inspections_all.i_id%TYPE := 593176;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005670;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005675;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005696;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005701;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005702;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005727;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005729;
--  lv_inspection_id    inspections_all.i_id%TYPE := 60005742;
  lv_inspection_id    inspections_all.i_id%TYPE := 60005750;
  --lv_inspection_id    inspections_all.i_id%TYPE := &&p_i_id;
  lv_user             VARCHAR2(32) := 'x999999';
  lv_raise_error      VARCHAR2(1) := 'Y';
  lv_rtc              PLS_INTEGER := -1;
BEGIN  

  ai_bcms_cii_returns_pkg.p_bcms_cii_add_inspection( p_inspection_id      => lv_inspection_id
                                                    ,p_user               => lv_user
                                                    ,p_raise_error        => lv_raise_error
                                                    ,p_rtc                => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -1) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('ADDED INSPECTION TO NEXT BATCH ' || lv_inspection_id);
  END IF;
  
END;

-- ADD A NUMBER OF INSPECTIONS TO NEXT BATCH
SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_inspection_count PLS_INTEGER := 10;
  lv_user             VARCHAR2(32) := 'x888888';
  lv_raise_error      VARCHAR2(1) := 'Y';
  lv_rtc              PLS_INTEGER := -1;
BEGIN  

  ai_bcms_cii_returns_pkg.p_bcms_cii_pop_inspections( p_inspection_count   => lv_inspection_count
                                                     ,p_user               => lv_user
                                                     ,p_raise_error        => lv_raise_error
                                                     ,p_rtc                => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -1) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('ADDED INSPECTIONS TO NEXT BATCH ' || lv_inspection_count);
  END IF;
  
END;


--SET MAX BATCH SIZE
SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_batch_size       PLS_INTEGER := 100;
  lv_raise_error      VARCHAR2(1) := 'Y';
  lv_rtc              PLS_INTEGER := -1;
BEGIN  

  ai_bcms_cii_returns_pkg.p_update_max_batch_size ( p_batch_size   => lv_batch_size
                                                   ,p_raise_error  => lv_raise_error
                                                   ,p_rtc          => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -1) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('Updated successful.');
  END IF;
  
END;
/


-- Files containing i_id=551434 on BAU :-
SIACS2008208S653ins.txt
SIACS2008208S653hol.txt
SIACS2008208S653ani.txt

SELECT * FROM inspections_All WHERE i_id = 550960;


-- BES HISTORY
SELECT s_description, created_date, created_by, bes_id, bes_ps_p_id, bes_ps_s_id, bes_brn, bes_inse_id, bes_start_date, bes_end_date, bes_reason_code, bes_reason_comment, bes_coma_id
FROM   bus_event_steps bes, steps st WHERE bes.bes_ps_s_id = st.s_id AND bes.bes_inse_id = (SELECT inse_id FROM inspection_events WHERE inse_parent_id = &&p_i_id) ORDER BY bes.bes_id DESC;

-- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** 
SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_raise_error   VARCHAR2(1) :='Y';
  lv_rtc           PLS_INTEGER := -1;
BEGIN  
 ai_bcms_cii_returns_pkg.p_bcms_cii_delete_data( p_raise_error   => lv_raise_error
                                                ,p_rtc           => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -2) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    NULL;
    --DBMS_OUTPUT.PUT_LINE('lv_rtc = ' || lv_rtc);
  END IF;
END;
/


-- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** 
-- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** 
SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_raise_error   VARCHAR2(1) :='Y';
  lv_rtc           PLS_INTEGER := -1;
BEGIN  
 ai_bcms_cii_returns_pkg.p_bcms_cii_pop_inspections( p_raise_error   => lv_raise_error
                                                 ,p_rtc           => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -2) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    NULL;
    --DBMS_OUTPUT.PUT_LINE('lv_rtc = ' || lv_rtc);
  END IF;
END;
/
-- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** 

-- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** 
SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_raise_error   VARCHAR2(1) :='Y';
  lv_rtc           PLS_INTEGER := -1;
BEGIN  
 ai_bcms_cii_returns_pkg.p_bcms_cii_pop_livestock  ( p_raise_error   => lv_raise_error
                                                 ,p_rtc           => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -2) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    NULL;
    --DBMS_OUTPUT.PUT_LINE('lv_rtc = ' || lv_rtc);
  END IF;
END;
/
-- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** -- ** PRE-REQUISITE ** 


SHOW ERROR

SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_raise_error   VARCHAR2(1) :='Y';
  lv_rtc           PLS_INTEGER := -1;
BEGIN  
 ai_bcms_cii_returns_pkg.p_bcms_cii_return_inspections( p_raise_error   => lv_raise_error
                                                    ,p_rtc           => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -2) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    NULL;
    --DBMS_OUTPUT.PUT_LINE('lv_rtc = ' || lv_rtc);
  END IF;
END;
/


SHOW ERROR

SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_raise_error   VARCHAR2(1) :='Y';
  lv_rtc           PLS_INTEGER := -1;
BEGIN  
 ai_bcms_cii_returns_pkg.p_bcms_cii_return_holdings( p_raise_error   => lv_raise_error
                                                    ,p_rtc           => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -2) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    NULL;
    --DBMS_OUTPUT.PUT_LINE('lv_rtc = ' || lv_rtc);
  END IF;
END;
/

SET SERVEROUTPUT ON SIZE 500000
DECLARE
  lv_raise_error   VARCHAR2(1) :='Y';
  lv_rtc           PLS_INTEGER := -1;
BEGIN  
 ai_bcms_cii_returns_pkg.p_bcms_cii_return_animals( p_raise_error   => lv_raise_error
                                                ,p_rtc           => lv_rtc);
                                                     
  IF    NVL(lv_rtc, -2) != 0 THEN
    DBMS_OUTPUT.PUT_LINE('The procedure didn' || '''' || 't work!.');
  ELSE
    NULL;
    --DBMS_OUTPUT.PUT_LINE('lv_rtc = ' || lv_rtc);
  END IF;
END;
/

--- AFTER 2540 SECS IN D605266AI: 
Error report -
ORA-01652: unable to extend temp segment by 128 in tablespace TEMP
ORA-06512: at D605266AI.AI_DATA_CAPTURE_PKG, line 6604
ORA-06512: at line 5
01652. 00000 -  unable to extend temp segment by %s in tablespace %s
*Cause:    Failed to allocate an extent of the required number of blocks for
           a temporary segment in the tablespace indicated.
*Action:   Use ALTER TABLESPACE ADD DATAFILE statement to add one or more
           files to the tablespace indicated.




SELECT DISTINCT 
	i_etas_unused_et_secure, 
	i_etas_unused_comm, 
	i_10pc_double_tag_check, 
	i_10pc_double_tag_comm
FROM inspections_all;


SELECT * FROM inspections_all WHERE  i_id = 551434
WHEREai_bcms_cii_returns_pkg.f_latest_bes_step (i_id, '6000, 6003' ) = 6000;


INSERT INTO bcms_cii_livestock
SELECT * FROM insp_animal_details
WHERE iad_i_id IN 
   ( SELECT bci_i_id
     FROM   bcms_cii_inspections);

SELECT COUNT(*) FROM bcms_cii_livestock;


DELETE
FROM bcms_cii_livestock;


SELECT * FROM requested_locations WHERE rl_i_id IN 
   ( SELECT bci_i_id
     FROM   bcms_cii_inspections);

  FUNCTION f_latest_bes_step (p_inspection_id IN inspections_all.i_id%TYPE
                             ,p_step_id_list  IN VARCHAR2 DEFAULT '6000, 6001, 6002, 6003');
                             
                             
   SELECT insp_id,
          SYSDATE,
          'I',
          'batchNo'
     FROM dual,
          (SELECT i_id insp_id
             FROM inspections_all i
            WHERE i_type = 'LI'
              AND i_status = 'C1'
              AND i_id >= 201614
              AND i_insp_capture_complete_ind = 'Y'
              AND i_id IN (SELECT rl_i_id
                             FROM requested_locations)
              AND NOT EXISTS (SELECT 'x'
                                FROM bcms_cii_results
                               WHERE bcr_i_id = i.i_id
                                 AND bcr_file_type = 'I')
              AND EXISTS (
                     SELECT 1
                       FROM inspections_locked il, inspection_letter_documents ild
                      WHERE ild.created_date < SYSDATE
                        AND ild.ild_type = 'RESULT'
                        AND ild.ild_i_id = i_id
                        AND il.il_i_id = i_id
                     HAVING MAX (ild.created_date) > MAX (il.created_date) ) );
                     
                     
DECLARE 

  clobtext CLOB := ''; 

  CURSOR c_get_holding_data IS
  SELECT bcrh_holding_detail
  FROM bcms_cii_return_holdings
  ORDER BY bcrh_line_no;
  
  CURSOR c_get_animal_data IS
  SELECT bcra_animal_detail
  FROM bcms_cii_return_animals
  ORDER BY bcra_line_no;
  
BEGIN

--  FOR holding_data IN c_get_holding_data LOOP
--        clobtext := clobtext 
--                             || holding_data.bcrh_holding_detail || CHR(13) || CHR(10); 
--
--  END LOOP;
  
  FOR animal_data IN c_get_animal_data LOOP
        clobtext := clobtext 
                             || animal_data.bcra_animal_detail || CHR(13) || CHR(10); 

  END LOOP;
  
--  INSERT INTO bcms_cii_return_data 
--  (	bcrd_id,
--    bcrd_file_type, 
--    bcrd_file_ref,
--    bcrd_data )
--  VALUES
--  ( 2,
--   'I',
--   '99',
--   clobtext
--  );

  INSERT INTO bcms_cii_return_data 
  (	bcrd_id,
    bcrd_file_type, 
    bcrd_file_ref,
    bcrd_data )
  VALUES
  ( 5,
   'A',
   '99',
   clobtext
  );
    
  dbms_output.Put_line('I have finished creating your clobs: Length = ' 
                       || Length(clobtext)); 
END; 
/


SELECT dbms_lob.substr(ild_document, 3000) FROM inspection_letter_documents WHERE  ild_id =  6;
SELECT * FROM inspection_letter_documents WHERE  ild_i_id =  &&p_i_id;


--DROP   TABLE bcms_cii_return_data; 
CREATE TABLE bcms_cii_return_data 
   (	created_date      DATE DEFAULT SYSDATE, 
    	created_by        VARCHAR2(32 BYTE) DEFAULT USER, 
      bcrd_id           NUMBER(8,0),
    	bcrd_file_type    VARCHAR2(1), 
    	bcrd_file_ref     VARCHAR2(30),
    	bcrd_data         CLOB,
    	CONSTRAINT bdrd_pk PRIMARY KEY (bcrd_id)
   );

ALTER TABLE bcms_cii_return_data
 ADD CONSTRAINT bcrd_chk1 CHECK(created_date IS NOT NULL)
 ENABLE;

ALTER TABLE bcms_cii_return_data
 ADD CONSTRAINT bcrd_chk2 CHECK(created_by IS NOT NULL)
 ENABLE;

ALTER TABLE bcms_cii_return_data
 ADD CONSTRAINT bcrd_chk3 CHECK(bcrd_file_type IS NOT NULL)
 ENABLE;

ALTER TABLE bcms_cii_return_data
 ADD CONSTRAINT bcrd_chk4 CHECK(bcrd_file_ref IS NOT NULL)
 ENABLE;

ALTER TABLE bcms_cii_return_data
ADD CONSTRAINT bcrd_chk5 CHECK 
(bcrd_file_type IN ('I', 'H', 'A'))
ENABLE;


-- 30,274

SELECT /*+ USE_HASH (c1, c_or) */ c1.cir_bov_eartag cii_tag
             ,      c1.cir_i_id       cii_insp
             ,      c1.cir_location_code cii_loc
             ,      decode(c1.cir_code,'FM1','FM','FM2','FM','DD1','DD',c1.cir_code)   cir_code
             ,      c_or.cir_code bad_code
            FROM cattle_insp_results c1
            ,    cattle_insp_results c_or
            WHERE c1.cir_bov_eartag = c_or.cir_bov_eartag(+)
            AND   c1.cir_i_id = c_or.cir_i_id (+)
            AND   c1.cir_hh_field = c_or.cir_hh_field(+)
            AND   nvl(c1.cir_location_code, '1234567890123') = nvl(c_or.cir_location_code (+)
                                                                  , nvl(c1.cir_location_code, '1234567890123'))
            AND   c1.cir_code NOT IN ('DEL','OE','BP','SP')
            AND   nvl(c1.cir_bov_eartag,'X') != 'X'
            AND   c_or.cir_code(+) ='OR'
            and c1.cir_i_id = 551434
            AND c_or.cir_code IS NOT NULL

CII_TAG                     CII_INSP                    CII_LOC                     CIR_CODE                    BAD_CODE                    
UK583723700832              568011                      82/502/0111                 OR                          OR                          
UK583723700832              568011                      82/502/0111                 FM                          OR                          
UK583723700832              568011                      82/500/0013                 OR                          OR                          
UK583723700832              568011                      82/500/0013                 FM                          OR                          
UK583723700832              568011                      82/502/0111                 DC                                                      
UK583723700832              568011                      82/502/0111                 DC                                                      
UK583723700832              568011                      82/502/0111                 DC                                                      
            
SELECT              c1.cir_bov_eartag cii_tag
             ,      c1.cir_i_id       cii_insp
             ,      c1.cir_location_code cii_loc
             ,      decode(c1.cir_code,'FM1','FM','FM2','FM','DD1','DD',c1.cir_code)   cir_code
             ,      c_or.cir_code bad_code
            FROM cattle_insp_results c1
            ,    cattle_insp_results c_or
            WHERE c1.cir_iad_id = c_or.cir_iad_id(+)
--            c1.cir_bov_eartag = c_or.cir_bov_eartag(+)
            AND   c1.cir_i_id = c_or.cir_i_id (+)
--            AND   c1.cir_hh_field = c_or.cir_hh_field(+)
--            AND   nvl(c1.cir_location_code, '1234567890123') = nvl(c_or.cir_location_code (+)
--                                                                  , nvl(c1.cir_location_code, '1234567890123'))
            AND   c1.cir_code NOT IN ('DEL','OE','BP','SP')
            AND   nvl(c1.cir_bov_eartag,'X') != 'X'
            AND   c_or.cir_code(+) ='OR' 
            and c1.cir_i_id = 551434
            AND c_or.cir_code IS NOT NULL
            
            
SELECT * FROM cattle_insp_results c1
WHERE   c1.cir_bov_eartag = 'UK583723700832'

              SELECT MAX(il.il_i_id)
                 FROM inspections_locked il, inspection_letter_documents ild
                WHERE ild.created_date < SYSDATE
                  AND ild.ild_type = 'RESULT'
                  AND ild.ild_i_id =  il.il_i_id
               HAVING MAX (ild.created_date) > MAX (il.created_date)


SELECT inspections_all.* FROM inspections_all, inspection_pack_documents
WHERE ipd_i_id = i_id
AND i_id IN (
        SELECT ipbd_i_id FROM inspection_pack_bus_details)
ORDER BY i_type, i_id DESC;


SELECT * FROM bcms_cii_results WHERE  bcr_i_id   =       &&p_i_id;


INSERT INTO  bcms_cii_inspections  
(  bci_id,
   bci_i_id)
SELECT ROWNUM,
       i_id insp_id
FROM   inspections_all I
WHERE  i_type = 'LI'
AND    i_status = 'C1'
AND    i_id >= 201614
AND    i_insp_capture_complete_ind = 'Y'
AND    i_id IN(SELECT rl_i_id
               FROM requested_locations)
AND    NOT EXISTS(SELECT 'x'
                 FROM bcms_cii_results
                 WHERE bcr_i_id = i.i_id
                 AND   bcr_file_type = 'H')
AND    EXISTS (
               SELECT 1
                 FROM inspections_locked il, inspection_letter_documents ild
                WHERE ild.created_date < SYSDATE
                  AND ild.ild_type = 'RESULT'
                  AND ild.ild_i_id = i_id
                  AND il.il_i_id = i_id
               HAVING MAX (ild.created_date) > MAX (il.created_date));
               
               
SELECT *
FROM bcms_cii_results
WHERE bcr_file_ref = 6267

UPDATE bcms_cii_results SET bcr_i_id = -551434 WHERE bcr_i_id = 551434;

    SELECT TO_CHAR(NVL(MAX(TO_NUMBER(bcr_file_ref)),0)) 
    FROM bcms_cii_results;
    
    DELETE FROM bcms_cii_return_data WHERE bcrd_file_ref = '9354';
    DELETE FROM bcms_cii_results     WHERE bcr_file_ref  = '9354';
    
SELECT * FROM cattle_insp_results WHERE cir_i_id = &&p_i_id;



        ,(SELECT i_id                                                             v0_i_id,
                 rl_location_code                                                 v0_rl_location_code,
                 rl_location_type,
                 i_10pc_double_tag_check,
                 i_etas_unused_et_secure,
                 'map_ref'                                                        map_ref
           FROM requested_locations
           ,    bcms_cii_inspection_queue
           ,    inspections_all
           WHERE rl_i_id  = bciq_i_id
           AND   i_id     = bciq_i_id
           AND   bciq_file_ref IS NULL)
           
         (SELECT  rcr_i_id                                              v5_i_id,
                  REPLACE(SUBSTR(rcr_c_loccode, 4), '/')                v5_rl_location_code,
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
           AND   bciq_file_ref IS NULL)
           
           
 CREATE TABLE D605266AI.RETURNED_CII_REQUEST 
   (	CREATED_DATE DATE DEFAULT sysdate NOT NULL ENABLE, 
	CREATED_BY VARCHAR2(32 BYTE) DEFAULT user NOT NULL ENABLE, 
	RCR_I_ID NUMBER(8,0) NOT NULL ENABLE, 
	RCR_C_LOCCODE VARCHAR2(17 BYTE) NOT NULL ENABLE, 
	RCR_C_LSTINSP DATE, 
	RCR_C_INSPYR NUMBER(4,0), 
	RCR_C_PREMTYPE VARCHAR2(25 BYTE), 
	RCR_C_SUBTYPE VARCHAR2(12 BYTE), 
	RCR_C_CONTACT VARCHAR2(54 BYTE), 
	RCR_C_LOC VARCHAR2(35 BYTE), 
	RCR_C_ADDR1 VARCHAR2(35 BYTE), 
	RCR_C_ADDR2 VARCHAR2(35 BYTE), 
	RCR_C_ADDR3 VARCHAR2(35 BYTE), 
	RCR_C_ADDR4 VARCHAR2(35 BYTE), 
	RCR_C_PCODE VARCHAR2(8 BYTE), 
	RCR_C_TEL VARCHAR2(25 BYTE), 
	RCR_C_MOBILE VARCHAR2(25 BYTE), 
	RCR_C_FAX VARCHAR2(25 BYTE), 
	RCR_C_EMAIL VARCHAR2(50 BYTE), 
	RCR_C_MAPREF VARCHAR2(9 BYTE), 
           
 --    ,(select substr(cdm_farm.descr(substr(rl_location_code,4)),1,60) contact_name   
  --      ,      rl_location_code
  --      ,      i_id 
  --      ,      i_inspected_date
  --      ,      i_10pc_double_tag_check
  --      ,      i_etas_unused_et_secure
  --      ,      cc.cpt_line1  addr1
  --      ,      cc.cpt_line2  addr2
  --      ,      cc.cpt_line3  addr3
  --      ,      cc.cpt_line4  addr4
  --      ,      cc.cpt_postcode post_code
  --      ,      pp.ppn_phone_no phone_no

SELECT i_pn_id, i_insp_comm FROM inspections_All WHERE i_id = 60003526; - 110623	929081
SELECT * FROM comments_all WHERE coma_id = 929081;

UPDATE inspections_All SET i_insp_comm = 929081 WHERE  i_id = 60003526;

SELECT substr(REPLACE(REPLACE(coma_text,chr(10),' '),'|',' '),1,1000)    comments
              ,i_id                                                               v6_i_id
              ,rl_location_code                                                   v6_loc
        FROM comments_all
        ,    requested_locations
        ,    inspections_all
        WHERE i_type = 'LI'
        AND i_insp_capture_complete_ind = 'Y'
        AND i_status = 'C1'
--        AND NOT EXISTS (SELECT 1
--                        FROM bcms_cii_results
--                        WHERE bcr_i_id = i_id
--                        AND   bcr_file_type = 'H')
        AND rl_i_id = i_id
        AND coma_id = i_insp_comm
        AND i_id = 60003526

    SELECT i_id
    ,      i_status
     ,     i_insp_capture_complete_ind
     ,     i_type
     FROM inspections_all
    WHERE i_id = &&p_i_id;

SELECT &&p_i_id FROM dual;

    SELECT created_date, sel_err_msg
    FROM std_err_log
    WHERE created_date > SYSDATE -(1/(24*60))
    ORDER BY sel_err_id DESC;

    SELECT sel1.created_date, sel1.sel_err_msg
    FROM std_err_log sel1
    WHERE sel1.sel_err_id = (SELECT MAX(sel2.sel_err_id) FROM std_err_log sel2);
    
    ORDER BY sel1.sel_err_id DESC;

    UPDATE inspections_all
     SET i_status = 'C1'
     , i_insp_capture_complete_ind = 'Y'
    WHERE i_id = &&p_i_id;
        
SELECT * FROM vw_lis_questions_and_answers, inspections_all WHERE i_type = 'LI' AND insp_id = i_id AND aq_id = 70000330 AND i_status = 'C1';

select * from inspections_all where i_id in (
60005670, 60005742, 60005727, 60005729, 60005696, 60005701, 60005702, 60005675, 60005677, 60005750
60005670, 60005742, 60005727, 60005729, 60005696, 60005701, 60005702, 60005675, 60005750
)

SELECT * FROM comments_all WHERE 
coma_id IN  (SELECT i_insp_comm FROM ci_ai_owner.inspections_all  WHERE i_id IN (  60005670, 60005742, 60005727, 60005729, 60005696, 60005701, 60005702, 60005675, 60005677, 60005750)
             UNION
             SELECT i_producers_comm FROM ci_ai_owner.inspections_all  WHERE i_id IN (  60005670, 60005742, 60005727, 60005729, 60005696, 60005701, 60005702, 60005675, 60005677, 60005750)
             )
      
      
70000330	BOVINS	<big><b>Storage of eartags</b></big><br><b>Are officially approved eartags stored securely?</b><br><i>If "No", inform producer eartags must be stored securely</i>

SELECT * FROM INSPECTIONS_ALL WHERE I_ID IN (
SELECT * FROM VW_LIS_QUESTIONS_AND_ANSWERS WHERE INSP_ID IN (
60005670,
60005675,
60005696,
60005701,
60005702,
60005727,
60005729,
60005742,
60005750);


REM INSERTING into INSPECTION_PACK_BUS_DETAILS
SET DEFINE OFF;
REM INSERTING into INSPECTION_PACK_BUS_DETAILS
SET DEFINE OFF;
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (4,593176,1,'AYR','753330096','Test IPBS Business Name',60005670,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (5,593176,1,'AYR','753330096','Test IPBS Business Name',60005675,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (6,593176,1,'AYR','753330096','Test IPBS Business Name',60005696,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (7,593176,1,'AYR','753330096','Test IPBS Business Name',60005701,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (8,593176,1,'AYR','753330096','Test IPBS Business Name',60005702,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (9,593176,1,'AYR','753330096','Test IPBS Business Name',60005727,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (10,593176,1,'AYR','753330096','Test IPBS Business Name',60005729,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (11,593176,1,'AYR','753330096','Test IPBS Business Name',60005742,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);
Insert into INSPECTION_PACK_BUS_DETAILS (IPBD_ID,IPBD_BRN,IPBD_AREA_OFFICE,IPBD_AREA_OFFICE_NAME,IPBD_MLC,IPBD_BUSINESS_NAME,IPBD_I_ID,IPBD_ADDRESS_LINE1,IPBD_ADDRESS_LINE2,IPBD_ADDRESS_LINE3,IPBD_ADDRESS_LINE4,IPBD_ADDRESS_LINE5,IPBD_POSTCODE,IPBD_MANAGEMENT_DISTRICT,IPBD_LANDLINE_TELECOM,IPBD_MOBILE_TELECOM,IPBD_START_DATE,IPBD_END_DATE,CREATED_DATE,CREATED_BY,IPBD_RESPONSIBLE_PERSON,IPBD_PREF_CONTACT_NAME,IPBD_PREF_ADDRESS_LINE1,IPBD_PREF_ADDRESS_LINE2,IPBD_PREF_ADDRESS_LINE3,IPBD_PREF_ADDRESS_LINE4,IPBD_PREF_ADDRESS_LINE5,IPBD_PREF_POSTCODE,IPBD_PREF_LANDLINE_TELECOM,IPBD_PREF_MOBILE_TELECOM) values (12,593176,1,'AYR','753330096','Test IPBS Business Name',60005750,'Test IPBS Address Line 1','Test IPBS Address Line 2',null,null,null,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),null,to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),'CI_AI_OWNER','Test Analyst',null,null,null,null,null,null,null,null,null);


SELECT * FROM INSPECTIONS_ALL WHERE I_ID = 612400

CREATE TABLE BCMS_CII_RETURN_ANIMAL_V13 AS SELECT * FROM BCMS_CII_RETURN_ANIMAL_FIELDS;
SELECT COUNT(*) FROM BCMS_CII_RETURN_ANIMAL_V13;

SELECT BCRAF_EARTAG_NO FROM BCMS_CII_RETURN_ANIMAL_FIELDS MINUS SELECT BCRAF_EARTAG_NO FROM BCMS_CII_RETURN_ANIMAL_V13;
SELECT BCRAF_EARTAG_NO FROM BCMS_CII_RETURN_ANIMAL_V13 MINUS SELECT BCRAF_EARTAG_NO FROM BCMS_CII_RETURN_ANIMAL_FIELDS;

SELECT BCRAF_EARTAG_NO, COUNT(*)  FROM BCMS_CII_RETURN_ANIMAL_FIELDS
WHERE BCRAF_I_ID  = 609273
GROUP BY BCRAF_EARTAG_NO
HAVING 0 < COUNT(*)
ORDER BY COUNT(*) DESC

SELECT BCRAF_EARTAG_NO, COUNT(*)  FROM BCMS_CII_RETURN_ANIMAL_V13
WHERE BCRAF_I_ID  = 609273
GROUP BY BCRAF_EARTAG_NO
HAVING 0 < COUNT(*)
ORDER BY COUNT(*) DESC

DELETE FROM BCMS_CII_RETURN_ANIMAL_V13
WHERE  BCRAF_I_ID =593176;

DELETE FROM BCMS_CII_RETURN_ANIMAL_FIELDS
WHERE  BCRAF_I_ID =593176;

SELECT * FROM VW_LIS_QUESTIONS_AND_ANSWERS WHERE INSP_ID IN (
SELECT BCIQ_I_ID FROM BCMS_CII_INSPECTION_QUEUE)
AND aq_id=70000330;

SELECT * FROM ALL_QUESTIONS WHERE aq_id=70000330

--  SGII
SELECT * FROM vw_lis_questions_and_answers WHERE insp_id = 99
AND aq_id=70000700;

SELECT qa1.insp_id insp_id, ai_breach_functions_pkg.fn_sgii_err_code30016_sect section, ai_breach_functions_pkg.fn_sgii_smr8_9_breach breach_code, sdfe_err_no error_num, sdfe_err_msg message FROM vw_lis_questions_and_answers qa1, std_defined_errors WHERE sdfe_err_system = 'SGII_SYSTEM' AND
sdfe_err_no = 30016 AND 
qa1.psq_id = 70000700 AND qa1.answer = 'No' 

    CURSOR c_get_smr8_error_codes IS
    SELECT section, breach_code 
    FROM vw_sgii_system_errors 
    WHERE insp_id = 99 --p_inspection_id
    AND breach_code IS NOT NULL
    ORDER BY 1;
    
    
SELECT * from VW_SGII_SYSTEM_ERRORS   WHERE insp_id = 99  

SELECT qa1.insp_id insp_id, ai_breach_functions_pkg.fn_sgii_err_code30016_sect section, ai_breach_functions_pkg.fn_sgii_smr8_9_breach breach_code, sdfe_err_no error_num, sdfe_err_msg message FROM vw_lis_questions_and_answers qa1, std_defined_errors WHERE sdfe_err_system = 'SGII_SYSTEM' AND
sdfe_err_no = 30016 AND 
qa1.psq_id = 70000700 AND qa1.answer = 'No' 
AND NOT EXISTS (SELECT 1 FROM vw_lis_questions_and_answers qa2 WHERE qa2.insp_id = 99 AND qa2.psq_id = 173957 AND qa2.answer = ai_breach_functions_pkg.fn_sgii_smr8_9_breach)


SELECT * FROM sit_ai_owner.std_defined_errors WHERE sdfe_err_no = 20054;

SELECT MAX(bcrd_file_ref) FROM bcms_cii_return_data;
SELECT MAX(TO_NUMBER(bcr_file_ref)) FROM uat_ai_owner.bcms_cii_results;


SELECT * FROM inspections_all where i_id = 612221
/


DELETE FROM bcms_cii_results where bcr_file_ref ='8409'


SELECT DISTINCT bcr_file_ref FROM bcms_cii_results where bcr_i_id in (
--SELECT * FROM inspections_all where i_id in (
603016,
603096,
603756,
604098,
604297,
604376,
604404,
604636,
604837,
604863,
605398,
605656,
605697,
605856,
606356,
606357,
606436,
606557,
606659,
606660,
606699,
606777,
606779,
606782,
606783,
606818,
606819,
606837,
606856,
606896,
606916,
606920,
606936,
606937,
606959,
606996,
607036,
607096,
607157,
607176)



-- ADD SPECIFIED INSPECTION TO NEXT BATCH
SET SERVEROUTPUT ON SIZE 500000
DECLARE

  CURSOR c_inspection_list IS
  SELECT 603016 insp_id FROM dual UNION
  SELECT 603096 FROM dual UNION
  SELECT 603756 FROM dual UNION
  SELECT 604098 FROM dual UNION
  SELECT 604297 FROM dual UNION
  SELECT 604376 FROM dual UNION
  SELECT 604404 FROM dual UNION
  SELECT 604636 FROM dual UNION
  SELECT 604837 FROM dual UNION
  SELECT 604863 FROM dual UNION
  SELECT 605398 FROM dual UNION
  SELECT 605656 FROM dual UNION
  SELECT 605697 FROM dual UNION
  SELECT 605856 FROM dual UNION
  SELECT 606356 FROM dual UNION
  SELECT 606357 FROM dual UNION
  SELECT 606436 FROM dual UNION
  SELECT 606557 FROM dual UNION
  SELECT 606659 FROM dual UNION
  SELECT 606660 FROM dual UNION
  SELECT 606699 FROM dual UNION
  SELECT 606777 FROM dual UNION
  SELECT 606779 FROM dual UNION
  SELECT 606782 FROM dual UNION
  SELECT 606783 FROM dual UNION
  SELECT 606818 FROM dual UNION
  SELECT 606819 FROM dual UNION
  SELECT 606837 FROM dual UNION
  SELECT 606856 FROM dual UNION
  SELECT 606896 FROM dual UNION
  SELECT 606916 FROM dual UNION
  SELECT 606920 FROM dual UNION
  SELECT 606936 FROM dual UNION
  SELECT 606937 FROM dual UNION
  SELECT 606959 FROM dual UNION
  SELECT 606996 FROM dual UNION
  SELECT 607036 FROM dual UNION
  SELECT 607096 FROM dual UNION
  SELECT 607157 FROM dual UNION
  SELECT 607176 FROM dual;
  
  lv_user             VARCHAR2(32) := 'x123456';
  lv_raise_error      VARCHAR2(1) := 'Y';
  lv_rtc              PLS_INTEGER := -1;
BEGIN  

  FOR j IN c_inspection_list LOOP

--    INSERT INTO inspection_pack_bus_details (ipbd_id,ipbd_brn,ipbd_area_office,ipbd_area_office_name,ipbd_mlc,ipbd_business_name,ipbd_i_id,ipbd_address_line1,ipbd_address_line2,ipbd_address_line3,ipbd_address_line4,ipbd_address_line5,ipbd_postcode,ipbd_management_district,ipbd_landline_telecom,ipbd_mobile_telecom,ipbd_start_date,ipbd_end_date,created_date,created_by,ipbd_responsible_person,ipbd_pref_contact_name,ipbd_pref_address_line1,ipbd_pref_address_line2,ipbd_pref_address_line3,ipbd_pref_address_line4,ipbd_pref_address_line5,ipbd_pref_postcode,ipbd_pref_landline_telecom,ipbd_pref_mobile_telecom) 
--    VALUES (3,593176,1,'AYR','753330096','Test IPBS Business Name',j.insp_id,'Test IPBS Address Line 1','Test IPBS Address Line 2',NULL,NULL,NULL,'TT99 9TT','TEST MAN DISTRICT','11111111111','2222222222',to_date('22-FEB-2016 00:00:00','DD-MON-RRRR HH24:MI:SS'),NULL,SYSDATE, USER,'Test Analyst',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
  
  DBMS_OUTPUT.PUT_LINE(j.insp_id);
  ai_bcms_cii_returns_pkg.p_bcms_cii_add_inspection( p_inspection_id      => j.insp_id
                                                    ,p_user               => lv_user
                                                    ,p_raise_error        => lv_raise_error
                                                    ,p_rtc                => lv_rtc);

  END LOOP;
    
END;
