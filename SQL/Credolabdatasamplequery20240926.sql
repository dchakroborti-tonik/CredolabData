WITH
  b AS (
  SELECT
    loanAccountNumber,
    min_inst_def30,
    obs_min_inst_def30
  FROM
    prj-prod-dataplatform.risk_credit_mis.loan_deliquency_data
  WHERE
    obs_min_inst_def30 >= 2),
lmt as
(SELECT
  lmt.loanAccountNumber,
  lmt.customerId,
  lmt.digitalLoanAccountId,
  lmt.tsa_onboarding_time,
  lmt.startApplyDateTime,
  lmt.termsAndConditionsSubmitDateTime,
  lmt.isTermsAndConditionsAccepted,
  lmt.disbursementDateTime,
  lmt.flagDisbursement,
  lmt.loanPaidStatus,
  case when b.obs_min_inst_def30 >=2 and b.min_inst_def30 in (1,2) then lmt.loanAccountNumber end FSPD30_loancnt,
  case when b.obs_min_inst_def30 >=2 then lmt.loanAccountNumber end obsFSPD30_loancnt
FROM
  `risk_credit_mis.loan_master_table` lmt
INNER JOIN
  b
ON
  lmt.loanAccountNumber = b.loanAccountNumber 
)
select 
distinct
  lmt.customerId,
  lmt.digitalLoanAccountId,
  lmt.loanAccountNumber,
  lmt.tsa_onboarding_time,
  lmt.startApplyDateTime,
  lmt.termsAndConditionsSubmitDateTime,
  lmt.isTermsAndConditionsAccepted,
  lmt.disbursementDateTime,
  lmt.flagDisbursement,
  lmt.loanPaidStatus,
  t3.creditScoreUpdated   ,
  t3.fraudScore   ,	
  t3.fraudScoreUpdated    ,
  t3.calculateddate   ,
  t4.run_date ,
  ca.package_name ,
  ca.first_install_time    ,
  ca.last_update_time      ,
  t4.GeneralInfo.brand     ,
  t4.Hardware.device__brand   ,
  t4.Hardware.device__manufacturer   ,
  t4.Hardware.device__model,
  t4.GeneralData.telephony_info__network_operator_name,
  t4.GeneralData.telephony_info__network_operator,
  t4.GeneralData.sim_operator_name,
  ptat.Category,
  -- ptat.Rating,
  case when ptat.Rating = 'rated for 3+' then 1 else 0 end rated_for_3_plus,
  case when ptat.Rating = 'rated for 7+' then 1 else 0 end rated_for_7_plus,
  case when ptat.Rating = 'rated for 12+' then 1 else 0 end rated_for_12_plus,
  case when ptat.Rating = 'rated for 16+' then 1 else 0 end rated_for_16_plus,
  case when ptat.Rating = 'rated for 18+' then 1 else 0 end rated_for_18_plus,
  case when ptat.Rating = 'undefined' then 1 else 0 end undefined,
  case when ptat.Rating = 'unrated' then 1 else 0 end unrated,
  case when ptat.Rating is null then 1 else 0 end Rating_Not_Available,
  ptat.Is_Paid,
  lmt.FSPD30_loancnt,     ---- FSPD30 = 1 when this value is not null(provided this as there were be duplicate rows in this dataset because of package name)
  lmt.obsFSPD30_loancnt   ---- obsFSPD30 = 1 when this value is not null (provided this as there were be duplicate rows in this dataset because of package name)
from lmt
LEFT JOIN
`prj-prod-dataplatform.dl_loans_db_raw.tdbk_digital_loan_application` t2
ON lmt.digitalLoanAccountId = t2.digitalLoanAccountId
LEFT JOIN
`prj-prod-dataplatform.dl_loans_db_raw.tdbk_credolab_track` t3
ON t2.credolabRefNumber = t3.refno
LEFT JOIN
`prj-prod-dataplatform.credolab_raw.android_credolab_datasets_struct_columns` t4
ON t3.refno = t4.deviceId
inner join
`prj-prod-dataplatform.core_raw.loan_accounts` loan
on loan.CUSTOMERID = lmt.customerId
 INNER JOIN
(select deviceId, af.package_name as package_name, af.first_install_time as first_install_time , af.last_update_time as last_update_time from `prj-prod-dataplatform.credolab_raw.android_credolab_Application`  ,
unnest(Application) as af) ca
ON ca.deviceId = t3.refno
LEFT JOIN prj-prod-dataplatform.dap_ds_poweruser_playground.PH_Tonikbank_Application_Temp ptat
ON REGEXP_REPLACE(ca.package_name, r'[ ._]', '') = REGEXP_REPLACE(ptat.Package_Name, r'[ ._]', '')
where date(lmt.startApplyDateTime) >='2024-06-01'   ---- Please change the date as per your requirement. This is Loan Application Apply Date
and lmt.FSPD30_loancnt is not null
order by lmt.customerId
limit 1000   --- Please remove this when running the query
;