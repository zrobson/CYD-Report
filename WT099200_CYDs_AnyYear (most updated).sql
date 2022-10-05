--TAB=CYDs
--Records=All
WITH MAIN_POOL AS (
   SELECT GIFT_DONOR_ID,
          PREF_MAIL_NAME,
          TRT.SHORT_DESC RECORD_TYPE
     FROM ENTITY E,
          TMS_RECORD_TYPE TRT, (
      SELECT GIFT_DONOR_ID
        FROM GIFT,
             ALLOCATION
       WHERE GIFT_YEAR_OF_GIVING = '&ThisFYR'
         AND GIFT_ASSOCIATED_ALLOCATION = ALLOCATION_CODE
         AND (ALLOC_SCHOOL = 'KM' OR
             (ALLOC_SCHOOL = 'LW' AND
              ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
      UNION
      SELECT PLEDGE_DONOR_ID
        FROM PLEDGE,
             ALLOCATION
       WHERE PLEDGE_YEAR_OF_GIVING = '&ThisFYR'
         AND PLEDGE_ALLOCATION_NAME = ALLOCATION_CODE
         AND (ALLOC_SCHOOL = 'KM' OR
             (ALLOC_SCHOOL = 'LW' AND
              ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM')))
    WHERE GIFT_DONOR_ID = ID_NUMBER
      AND E.RECORD_TYPE_CODE = TRT.RECORD_TYPE_CODE),
               
   TOTAL_GIFT_&ThisFYR AS (           
   SELECT GIFT_DONOR_ID,
          SUM(GIFT_ASSOCIATED_CREDIT_AMT) TOTAL_GIFT_FY_&ThisFYR
     FROM GIFT,
          ALLOCATION
    WHERE GIFT_YEAR_OF_GIVING = '&ThisFYR'
      AND GIFT_ASSOCIATED_ALLOCATION = ALLOCATION_CODE
      AND (ALLOC_SCHOOL = 'KM' OR
          (ALLOC_SCHOOL = 'LW' AND
           ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
    GROUP BY GIFT_DONOR_ID),
    
   TOTAL_PLEDGE_&ThisFYR AS (           
   SELECT PLEDGE_DONOR_ID,
          SUM(PLEDGE_ASSOCIATED_CREDIT_AMT) TOTAL_PLEDGE_FY_&ThisFYR
     FROM PLEDGE,
          ALLOCATION
    WHERE PLEDGE_YEAR_OF_GIVING = '&ThisFYR'
      AND PLEDGE_ALLOCATION_NAME = ALLOCATION_CODE
      AND (ALLOC_SCHOOL = 'KM' OR
          (ALLOC_SCHOOL = 'LW' AND
           ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
    GROUP BY PLEDGE_DONOR_ID),
    
   MOST_RECENT_GIFT AS (
   SELECT GIFT_DONOR_ID,
          MAX(GIFT_DATE_OF_RECORD) MOST_RECENT_GIFT
     FROM GIFT,
          ALLOCATION
    WHERE GIFT_ASSOCIATED_ALLOCATION = ALLOCATION_CODE
      AND (ALLOC_SCHOOL = 'KM' OR
          (ALLOC_SCHOOL = 'LW' AND
           ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
    GROUP BY GIFT_DONOR_ID),

   MOST_RECENT_PLEDGE AS (
   SELECT PLEDGE_DONOR_ID,
          MAX(PLEDGE_DATE_OF_RECORD) MOST_RECENT_PLEDGE
     FROM PLEDGE,
          ALLOCATION
    WHERE PLEDGE_ALLOCATION_NAME = ALLOCATION_CODE
      AND (ALLOC_SCHOOL = 'KM' OR
          (ALLOC_SCHOOL = 'LW' AND
           ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
    GROUP BY PLEDGE_DONOR_ID),
    
   PRIOR_FY_GIFT AS (
   SELECT GIFT_DONOR_ID,
          PRIOR_FY_GIFT_AMT,
          PRIOR_FY_GIFT_YEAR
     FROM (
      SELECT GIFT_DONOR_ID,
             SUM(GIFT_ASSOCIATED_CREDIT_AMT) PRIOR_FY_GIFT_AMT,
             GIFT_YEAR_OF_GIVING PRIOR_FY_GIFT_YEAR,
             ROW_NUMBER () OVER (PARTITION BY GIFT_DONOR_ID
                                     ORDER BY GIFT_YEAR_oF_GIVING DESC) ROWX
        FROM GIFT,
             ALLOCATION
       WHERE GIFT_YEAR_OF_GIVING < '&ThisFYR'
         AND GIFT_ASSOCIATED_ALLOCATION = ALLOCATION_CODE
         AND (ALLOC_SCHOOL = 'KM' OR
             (ALLOC_SCHOOL = 'LW' AND
              ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
       GROUP BY GIFT_DONOR_ID,
                GIFT_YEAR_OF_GIVING)
    WHERE ROWX = 1),
    
   PRIOR_FY_PLEDGE AS (
   SELECT PLEDGE_DONOR_ID,
          PRIOR_FY_PLEDGE_AMT,
          PRIOR_FY_PLEDGE_YEAR
     FROM (
      SELECT PLEDGE_DONOR_ID,
             SUM(PLEDGE_ASSOCIATED_CREDIT_AMT) PRIOR_FY_PLEDGE_AMT,
             PLEDGE_YEAR_OF_GIVING PRIOR_FY_PLEDGE_YEAR,
             ROW_NUMBER () OVER (PARTITION BY PLEDGE_DONOR_ID
                                     ORDER BY PLEDGE_YEAR_oF_GIVING DESC) ROWX
        FROM PLEDGE,
             ALLOCATION
       WHERE PLEDGE_YEAR_OF_GIVING < '&ThisFYR'
         AND PLEDGE_ALLOCATION_NAME = ALLOCATION_CODE
         AND (ALLOC_SCHOOL = 'KM' OR
             (ALLOC_SCHOOL = 'LW' AND
              ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
       GROUP BY PLEDGE_DONOR_ID,
                PLEDGE_YEAR_OF_GIVING)
    WHERE ROWX = 1),

    KSM_GIFT_TOTAL AS (    
    SELECT GIFT_DONOR_ID,
           SUM(GIFT_ASSOCIATED_CREDIT_AMT) KSM_GIFT_TOTAL
      FROM GIFT,
           ALLOCATION
     WHERE GIFT_ASSOCIATED_ALLOCATION = ALLOCATION_CODE
       AND (ALLOC_SCHOOL = 'KM' OR
           (ALLOC_SCHOOL = 'LW' AND
           ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
     GROUP BY GIFT_DONOR_ID),
    
    KSM_PLEDGE_TOTAL AS (    
    SELECT PLEDGE_DONOR_ID,
           SUM(PLEDGE_ASSOCIATED_CREDIT_AMT) KSM_PLEDGE_TOTAL
      FROM PLEDGE,
           ALLOCATION
     WHERE PLEDGE_ALLOCATION_NAME = ALLOCATION_CODE
       AND (ALLOC_SCHOOL = 'KM' OR
           (ALLOC_SCHOOL = 'LW' AND
           ALLOC_SCHOOLX(ALLOCATION_CODE, ALLOC_SCHOOL, 'KM') = 'KM'))
     GROUP BY PLEDGE_DONOR_ID)
     
     , last_gift_hh AS (
     SELECT   hh.ID_NUMBER
              , max(hh.tx_gypm_ind) keep(dense_rank First Order By DATE_OF_RECORD Desc) as last_gift_tx_gypm_ind
              , max(hh.tx_number) keep(dense_rank First Order By DATE_OF_RECORD Desc) AS last_gift_tx_number
              , max(hh.transaction_type) keep(dense_rank First Order By DATE_OF_RECORD Desc) AS last_transaction_type
              , max(hh.DATE_OF_RECORD) keep(dense_rank First Order By DATE_OF_RECORD Desc) AS last_gift_date
              , max(hh.RECOGNITION_CREDIT) keep(dense_rank First Order by DATE_OF_RECORD Desc) as last_gift_recognition_credit
              , max(hh.alloc_short_name) keep(dense_rank First Order by DATE_OF_RECORD Desc) as last_gift_allocation
     FROM RPT_PBH634.v_ksm_giving_trans_hh hh
     -- recognition credit greater than 0 and either a gift or pledge
     WHERE RECOGNITION_CREDIT > 0 AND (tx_gypm_ind LIKE 'G' OR tx_gypm_ind LIKE 'P')
     group by id_number
     )
     
     
     -- prospect manager
, prospect_manager AS (
SELECT h.id_number
      ,listagg(h.assignment_id_number, chr(13)) Within Group (order by h.assignment_report_name) as prospect_manager_id
      ,listagg(h.assignment_report_name, chr(13)) Within Group (order by h.assignment_report_name) as prospect_manager 
FROM rpt_pbh634.v_assignment_history h
INNER JOIN entity e ON h.id_number = e.id_number
WHERE assignment_type = 'PM'
AND assignment_active_calc = 'Active'
GROUP BY h.id_number
)

-- LGO
, lgo AS (
SELECT h.id_number
      ,listagg(h.assignment_id_number, chr(13)) Within Group (order by h.assignment_report_name) as lgo_id
      ,listagg(h.assignment_report_name, chr(13)) Within Group (order by h.assignment_report_name) as lgo 
FROM rpt_pbh634.v_assignment_history h
INNER JOIN entity e ON h.id_number = e.id_number
WHERE assignment_type = 'LG'
AND assignment_active_calc = 'Active'
GROUP BY h.id_number
)


SELECT MP.GIFT_DONOR_ID,
       MP.PREF_MAIL_NAME,
       prospect_manager.prospect_manager,
       lgo.lgo,
       TOTAL_GIFT_&ThisFYR.TOTAL_GIFT_FY_&ThisFYR,
       TOTAL_PLEDGE_&ThisFYR.TOTAL_PLEDGE_FY_&ThisFYR,
       last_gift_hh.last_gift_date,     
       last_gift_hh.last_gift_recognition_credit,
       last_gift_hh.last_gift_allocation,
       last_gift_hh.last_transaction_type,        
       MOST_RECENT_PLEDGE.MOST_RECENT_PLEDGE,
       PRIOR_FY_GIFT.PRIOR_FY_GIFT_AMT,
       PRIOR_FY_GIFT.PRIOR_FY_GIFT_YEAR,
       PRIOR_FY_PLEDGE.PRIOR_FY_PLEDGE_AMT,
       PRIOR_FY_PLEDGE.PRIOR_FY_PLEDGE_YEAR,
       WT0_PKG.GETKGSMYEAR(MP.GIFT_DONOR_ID) KSM_YEAR,
       MP.RECORD_TYPE,
       WT0_PKG.GETKGSMPROG(MP.GIFT_DONOR_ID) KSM_PROG,
       KSM_GIFT_TOTAL,
       KSM_PLEDGE_TOTAL      
       
  FROM MAIN_POOL MP,
       TOTAL_GIFT_&ThisFYR,
       TOTAL_PLEDGE_&ThisFYR,
       MOST_RECENT_GIFT,
       MOST_RECENT_PLEDGE,
       PRIOR_FY_GIFT,
       PRIOR_FY_PLEDGE,
       KSM_GIFT_TOTAL,
       KSM_PLEDGE_TOTAL,
       last_gift_hh,
       prospect_manager,
       lgo
 WHERE MP.GIFT_DONOR_ID = TOTAL_GIFT_&ThisFYR.GIFT_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = TOTAL_PLEDGE_&ThisFYR.PLEDGE_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = MOST_RECENT_GIFT.GIFT_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = MOST_RECENT_PLEDGE.PLEDGE_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = PRIOR_FY_GIFT.GIFT_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = PRIOR_FY_PLEDGE.PLEDGE_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = KSM_GIFT_TOTAL.GIFT_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = KSM_PLEDGE_TOTAL.PLEDGE_DONOR_ID (+)
   AND MP.GIFT_DONOR_ID = last_gift_hh.id_number (+)
   AND MP.GIFT_DONOR_ID = prospect_manager.id_number (+)
   AND MP.GIFT_DONOR_ID = lgo.id_number (+)
 ORDER BY MP.GIFT_DONOR_ID
