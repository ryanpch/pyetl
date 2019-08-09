

DECLARE @End AS DATE 
DECLARE @Start AS DATE 

set @End = EOMonth(DATEADD(Month,-1,GETDATE())); 

set @Start = EOMonth(DATEADD(Month,-15, @end))




/* Members */ 


if object_id('tempdb..#BaseMembers') is not null
drop table #BaseMembers

select distinct case when dm.MemberKey like '999999%' then concat(dm.MemberKey,DM.plankey) 
                     else dm.memberkey 
			    end as Memberkey
                ,m.EndOfMonthDate as MonthEndDate
				,m.plankey as PlanKey
				,h.ParentPlan as ParentPlanKey
				,dm.MemberDimID as MemberDimID
				,h.SegmentRating as SegmentRating
				,case when M.memberkey not like '999999%' then 1 else 0 end as MemberIndicator  ---this will always set to 1 if this query only return members
into #BaseMembers
from dbo.syn_vw_ClientPlanAssociationLookup_All m  
     inner join dbo.syn_vwop_MemberDim_All dm  
       on m.PlanKey = dm.PlanKey 
	      and dm.MemberKey = m.MemberKey
     inner join retl.vw_Employer_ActiveEmployerPlanHierarchy h  
       on m.PlanKey = h.PlanExternalPK
where m.EndOfMonthDate > @start 
      and m.EndOfMonthDate <= @end
      and m.memberkey not like '999999%'   ---disable this constraint if you want to include members and non-members
	  and dm.DeletedDate is null  --Remove any deleted or merged members
      --and m.Indicator = 'Y'  ---choose members that are with a current active plan

create nonclustered index ncix_MonthEndDate on #BaseMembers(MonthEndDate) include (MemberDimID, MemberKey)

/*******************************************************************************************************************************************************************
-- ORIGINAL QUERY COMPILED BY KEVAL DESAI --
Returns a list of Appian Claims and (AIA Claims that may not have found their way into Appian Claims table yet). AIA Claims are apparently uploaded by file each
Month and at some stage make their was into the Appian Claims table. If the MemberNumber and ClaimType (Death, IP) record exist in both the Appian Claim table and 
the Claim.AIA_Mos.Data table, the Appian Claim record is used, else the Claim.AIA_Mos.Data record is used. 
Changes from original query are:
1. AdmittedDate and Rejected Date changed. If InsurerDecision is 'declined' then Rejected Date will be populated with InsurerDecisionDate, if accepted then 
AdmittedDate will be populated with InsurerDecsionDate.
2. Added Occupation
3. Commented out the left join to the statushistory table to look up the AdmittedDate (due to 1 above).
4. Added left join to [Claim].[vw_Case] and [dbo].[vw_LookupOccupation_Current] to look up the occupation description
5. Changed SumInsured from CLG.OriginalCoverAmount  to INC.OriginalCoverAmount (********** possibly should be INC.UpdatedCoverAmount **********)
6. UpdatedCoverAmount added as UpdatedSumInsured
*******************************************************************************************************************************************************************/

IF OBJECT_ID('tempdb..#CLAIMSDATA') IS NOT NULL
DROP TABLE #CLAIMSDATA
SELECT *
INTO #CLAIMSDATA 
FROM (
	
	/********** Appian Claims details if exists, else AIA MOS Details from full outer join **********/
	SELECT 
		cmd.ConvertedClaimID
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.LastContributingEmployerNumber ELSE CL.LastContributingEmployerNumber END as EmployerNum
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.FundMemberNumber ELSE CMD.MemberNumber END AS MemberNumber
		,LIG.InsuranceGroupCode -- Pending>> for AIA claims
		,CL.ClaimID
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.DateOfBirth ELSE CON.BirthDate END AS BirthDate
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.Gender ELSE LG.GenderDescription END AS Gender
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.ClaimType ELSE SUBSTRING(LPCT.ClaimEventDescription, 1, LEN(LPCT.ClaimEventDescription) - CHARINDEX(' ', REVERSE(LPCT.ClaimEventDescription)))  END AS ClaimEventType
		,CASE
			WHEN CL.ClaimID IS NULL THEN 
				CASE 
					WHEN AIA.ClaimType LIKE 'Income Protection' THEN AIA.SumInsured * 12 
					ELSE AIA.SumInsured 
				END
			 ELSE INC.OriginalCoverAmount 
		END AS SumInsured
		,CASE
			WHEN CL.ClaimID IS NULL THEN 
				CASE 
					WHEN AIA.ClaimType LIKE 'Income Protection' THEN AIA.SumInsured * 12 
					ELSE AIA.SumInsured 
				END
			 ELSE INC.UpdatedCoverAmount  
		END AS UpdatedSumInsured
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.SumInsured ELSE INC.InsurerAcceptedAmount END AS InsurerAcceptedAmount
		,INC.InsurerDecisionDate
		,LID.InsurerDecisionDescription
		,CASE WHEN CL.ClaimID IS NULL THEN 'AIA' ELSE CLG.InsurerName END AS Insurer
		,IP.[SequentialNumber] as NumOfIPBenefitsPaid 
		, CASE WHEN CL.ClaimID IS NULL THEN AIA.ClaimStatus ELSE LPCS.ClaimStatusDescription END as Status
		, CASE 
			WHEN CL.ClaimID IS NULL THEN 
				CASE WHEN AIA.ClaimStatus LIKE 'Closed' THEN AIA.ClaimClosed ELSE AIA.ReportingMonth END
			ELSE CSH.StatusUpdatedDate
		 END AS StatusUpdatedDate
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.ClosedReason ELSE LCR.ClosureReasonDescription END AS ClosedReason
		--, Number of AIA IP Cliams Matched with DB
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.AssessedLoss ELSE CON.ClaimEventDate END AS Date_Of_Occurance
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.Lodged  ELSE CL.ClaimNotifiedDate END AS Notification_Date
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.Lodged  ELSE CFR.LastClaimFormReceived END AS Claim_Form_Received_Date
		
		--,CASE WHEN CL.ClaimID IS NULL THEN AIA.Lodged ELSE AD.AdmittedDate END as AdmittedDate   -- replaced by below 26/07/2017
		, CASE 
			WHEN CL.ClaimID IS NULL THEN AIA.Lodged ELSE
				CASE 
					WHEN INC.InsurerDecisionID IN (1,2) THEN INC.InsurerDecisionDate    -- full or partial accepted
					ELSE NULL
				END 
			END AS AdmittedDate
		--,CASE WHEN CL.ClaimID IS NULL AND AIA.Decision LIKE 'Declined' THEN AIA.ClaimClosed WHEN CL.ClosureReasonID = 2 THEN CL.DateofClosure ELSE NULL END as Rejected  -- replaced by below 26/07/2017
		,CASE 
			WHEN CL.ClaimID IS NULL AND AIA.Decision LIKE 'Declined' THEN AIA.ClaimClosed ELSE
				CASE 
					WHEN INC.InsurerDecisionID = 3 THEN INC.InsurerDecisionDate   -- declined
					ELSE NULL
				END
			END AS Rejected
		,CL.DateFirstPayment -- Pending >> For AIA Claims
		--, Cause Of Cessation -- Pending >> This looks same as ClosureReasons
		,CON.EventClassificationDescription --mulitple options availabe for this field
		-----------------, CON.SecondaryEventClassificationDescription
		,CON.SubClassificationDescription
		-----------------, CON.SecondarySubClassificationDescription
		,AIA.AIA_ClaimNumber
		--, Has_Insurance (Yes/No)
		,IP.PaymentDate as LastIP_PaymentDate 
		,IP.BenefitEndDate 
		,INC.IPMonthlyBenefit
		--, IPStatus --Do we need this? Its already covered in ClaimStatus
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.[DateJoinedEmployer] ELSE  CL.[DateJoinedEmployer] END as [DateJoinedEmployer]
		,CASE 
			WHEN CL.ClaimID IS NULL THEN 
				CASE 
					WHEN AIA.[DateLeftEmployer] = GETDATE() THEN NULL 
					ELSE Convert(Date, AIA.[DateLeftEmployer]) 
				END
			ELSE CL.[DateLeftEmployer] 
		END as [DateLeftEmployer]
		,CASE WHEN CL.ClaimID IS NULL THEN 1 ELSE CL.EmployerAtClaimFlag END AS AppianClaimsData_EmployerAtClaimFlag
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.FundJoinedDate ELSE SM.FundJoinedDate END AS FundJoinedDate
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.FundLeftDate ELSE SM.FundLeftDate END AS FundLeftDate
		,DeathDate
		,DisablementDate
		,Case When DeathDate is not Null or DeathDate>=DisablementDate Then DeathDate Else DisablementDate End as DateOfDeathOrDisability
		,REPLACE(REPLACE(REPLACE(REPLACE(Cast(CON.FileNoteText As Varchar(3500)), CHAR(10) + CHAR(13), ' '),CHAR(10), ' '), CHAR(13), ' '),CHAR(09), ' ')  as Cause	
		,CL.ClaimCaseID
		,CASE WHEN CL.ClaimID IS NULL THEN AIA.Occupation ELSE OCC.OccupationDescription END AS Occupation

	FROM rep0AppianApplicationData.Claim.vw_Claim_Member_Detail CMD
		INNER JOIN rep0AppianApplicationData.Claim.vw_Claim_Grid CLG
			ON CMD.ClaimID = CLG.ClaimID
		INNER JOIN rep0AppianApplicationData.Claim.vw_Claim CL
			ON CMD.ClaimID = CL.ClaimID
		INNER JOIN rep0AppianApplicationData.Claim.vw_Case AS CA
			ON CL.ClaimCaseID = CA.ClaimCaseID
		INNER JOIN rep0AppianApplicationData.Claim.vw_LookupClaimEventType_All LPCT
			ON CL.ClaimEventTypeID = LPCT.ClaimEventTypeID
		INNER JOIN rep0AppianApplicationData.Claim.vw_LookupClaimStatus_All LPCS
			ON CL.ClaimStatusID = LPCS.ClaimStatusID
		LEFT JOIN (
			SELECT ClaimID, UpdatedClaimStatusID, MAX(UpdatedDateTime) as StatusUpdatedDate 
			FROM rep0AppianApplicationData.Claim.vw_ClaimStatusHistory
			GROUP BY ClaimID, UpdatedClaimStatusID
			) CSH
		ON CL.ClaimID = CSH.ClaimID AND CL.ClaimStatusID = CSH.UpdatedClaimStatusID
		
		
		LEFT JOIN rep0AppianApplicationData.Claim.vw_LookupClaimClosureReason_All LCR
			ON CL.ClosureReasonID = LCR.ClosureReasonID
		LEFT JOIN (
			SELECT ClaimID, MAX(DateClaimFormsReceived) as LastClaimFormReceived 
			FROM rep0AppianApplicationData.Claim.vw_ClaimFormSent 
			GROUP BY ClaimID
			) CFR
			ON CL.ClaimID = CFR.ClaimID
		LEFT JOIN rep0AppianApplicationData.Claim.vw_InsuranceComponent INC   
			ON CL.ClaimID = INC.ClaimID
		LEFT JOIN rep0AppianApplicationData.Insurance.vw_InsurancePolicy INP
			ON INC.InsurancePolicyID = INP.InsurancePolicyID
		LEFT JOIN rep0AppianApplicationData.Insurance.vw_LookupInsuranceGroup_All LIG
			ON INP.InsuranceGroupID = LIG.InsuranceGroupID
		LEFT JOIN rep0AppianApplicationData.Claim.vw_LookupInsurerDecision_Current (nolock) as LID
			ON INC.InsurerDecisionID = LID.InsurerDecisionID
		INNER JOIN rep0AppianApplicationData.Claim.vw_Case_Context CON
		ON CL.ClaimCaseID = CON.ClaimCaseID
		INNER JOIN rep0AppianApplicationData.History.vw_SnapshotPerson SP
			ON CON.CurrentProcessInstanceID = SP.ProcessInstanceID
		INNER JOIN rep0AppianApplicationData.Customer.vw_LookupGender LG
			ON SP.GenderID = LG.GenderID
		LEFT JOIN rep0AppianApplicationData.dbo.vw_LookupOccupation_Current AS OCC
			ON CA.OccupationID = OCC.OccupationID
		INNER JOIN rep0AppianApplicationData.History.vw_SnapshotMember SM
			ON SP.PersonID = SM.PersonID
		LEFT JOIN (
			SELECT * 
			FROM (
				SELECT ROW_NUMBER() OVER ( PARTITION BY [ClaimID] ORDER BY [PaymentDate] Desc) RowNum
				,ClaimID ,[Gross_Payment_Amt] ,[PaymentDate] ,[BenefitStartDate] ,[BenefitEndDate] ,[InputSource] ,[SequentialNumber]
				FROM rep0AppianApplicationData.Claim.vw_IPPaymentHistory
				) Z 
			WHERE RowNum = 1
			) IP
	     ON CL.ClaimID = IP.ClaimID
	
	/********** AIA MOS Claims data **********/
		FULL OUTER JOIN (
	
				SELECT [AIAMosID]
				  ,[PolicyNumber]
				  ,[FundMemberNumber]
				  ,[AIA_ClaimNumber]
				  ,[NameOfInsured]
				  ,CASE 
					WHEN BenefitClaimType LIKE 'Death Benefit' THEN 'Death' 
					WHEN BenefitClaimType LIKE 'TPD Assist' THEN 'Total & Permanent Disablement'
					ELSE BenefitClaimType 
					END AS ClaimType
				  ,[Lodged]
				  ,[SumInsured]
				  ,[WaitingPeriod]
				  ,[BenefitPeriod]
				  ,[ClaimsEscalation]
				  ,[Gender]
				  ,[Occupation]
				  ,[CauseOfClaim]
				  ,[ICDCauseCode]
				  ,[ICDCauseChapter]
				  ,[DateOfBirth]
				  ,[AssessedLoss]
				  ,[OccupationClass]
				  ,[OccupationType]
				  ,[SmokerStatus]
				  ,[ClaimStatus]
				  ,[ClaimClosed]
				  ,[ClosedReason]
				  ,[Decision]
				  ,[DecisionReason]
				  ,[TerminalIllness]
				  ,[AgeAsAtLossDate]
				  ,[PaidToDate]
				  ,[LastContributingEmployerNumber]
				  ,[DateJoinedEmployer]
				  , CASE WHEN [DateLeftEmployer] IS NULL THEN GETDATE() ELSE [DateLeftEmployer] END AS [DateLeftEmployer]
				  ,IMP.ReportingMonth
				  , FundJoinedDate
				  , FundLeftDate
				  , RowNum
				FROM (
					SELECT 
						MD.[MemberNumber]
						, [LastContributingEmployerNumber]
						, [DateJoinedEmployer]
						--, CASE WHEN [DateLeftEmployer] IS NULL THEN GETDATE() ELSE [DateLeftEmployer] END AS [DateLeftEmployer]
						, CASE WHEN ([DateLeftEmployer] IS NULL or ([DateLeftEmployer] not between '19000101' and GetDate())) THEN Convert(Date,GETDATE()) ELSE Convert(Date,[DateLeftEmployer]) END AS [DateLeftEmployer]
						, SM.FundJoinedDate
						, SM.FundLeftDate
						, ROW_NUMBER() OVER (PARTITION BY MD.[MemberNumber] ORDER BY [DateJoinedEmployer]) as RowNum
					FROM rep0AppianApplicationData.Claim.vw_Claim_Member_Detail MD
						INNER JOIN rep0AppianApplicationData.History.vw_SnapshotMember SM 
							ON MD.MemberNumber = SM.MemberNumber
					where [DateJoinedEmployer] is not null
					group by MD.MemberNumber, [LastContributingEmployerNumber], [DateJoinedEmployer], [DateLeftEmployer], SM.FundJoinedDate, SM.FundLeftDate
				  ) Z
				LEFT JOIN (
					SELECT * 
					FROM rep0AppianApplicationData.Claim.AIA_Mos_Data ALLDATA 
						INNER JOIN (
									SELECT MAX(ImportID) as ImportID1 
									FROM rep0AppianApplicationData.Claim.AIA_Mos_Data
									) LASTDATA 
						  ON ALLDATA.ImportID = LASTDATA.ImportID1
						) AIA1 
				  ON Z.MemberNumber = AIA1.FundMemberNumber
				INNER JOIN rep0AppianApplicationData.Claim.AIAImportHistory IMP 
				  ON AIA1.ImportID1 = IMP.AIAImportID
				WHERE AIA1.AIAMosID IS NOT NULL
					  AND AIA1.Lodged BETWEEN [DateJoinedEmployer] AND [DateLeftEmployer]
			) AIA
		  ON AIA.FundMemberNumber = clg.MemberNumber 
			 AND AIA.ClaimType = SUBSTRING(clg.ClaimEventDescription, 1, LEN(clg.ClaimEventDescription) - CHARINDEX(' ', REVERSE(clg.ClaimEventDescription)))

		LEFT JOIN (
			SELECT [EmployerNumber], [EmployerName] 
			FROM rep0AppianApplicationData.dbo.vw_LookupEmployer
			) EMPN 
		  ON AIA.[LastContributingEmployerNumber] = EMPN.[EmployerNumber]
) EMPALL


--Grab only the claims that relate to the members we are interested in which have the status changes added in

IF OBJECT_ID('tempdb..#ClaimDetail') IS NOT NULL
DROP TABLE #ClaimDetail
SELECT ClaimID, EmployerNum, MemberNumber, ClaimEventType, ClosedReason, [status], StatusNow,
       CASE WHEN MonthEndDate IS NULL THEN StatusUpdatedDate ELSE MonthEndDate END AS MonthEndDate
INTO #ClaimDetail
FROM
	(SELECT DISTINCT CD.ClaimID, EmployerNum, MemberNumber, ClaimEventType, ClosedReason, MIN(EOMonth(CSH.CreatedDateTime)) AS MonthEndDate, CD.[Status], 
	EOMOnth(CD.StatusUpdatedDate) AS StatusUpdatedDate, CSH.UpdatedClaimStatusID AS StatusNow
	FROM #CLAIMSDATA CD
	--Not all claims have a history if they have come from AIA
		 LEFT JOIN rep0AppianApplicationData.Claim.vw_ClaimStatusHistory CSH ON CD.ClaimID = CSH.ClaimID
	WHERE EXISTS (SELECT NULL
					FROM #BaseMembers BM
					WHERE CD.MemberNumber = BM.MemberKey
					AND CD.EmployerNum = BM.PlanKey)
	--Only claims that the member was joined to the employer at the time of the claim (Some claims were before the member joined the employer or after they left them)
	AND AppianClaimsData_EmployerAtClaimFlag = '1'
	AND CD.ClaimID IS NOT NULL
	GROUP BY CD.ClaimID, EmployerNum, MemberNumber, ClaimEventType, ClosedReason, CD.[Status], CD.StatusUpdatedDate, CSH.UpdatedClaimStatusID
) RES


--**pyetl1
SELECT bm.MemberKey
       ,bm.MonthEndDate
	   ,bm.PlanKey 
	   ,bm.ParentPlanKey
	   ,bm.SegmentRating
	   ,cd.ClaimEventType
	   ,cd.ClosedReason
	   ,bm.MemberDimID
	   ,cd.ClaimID
       ,CASE WHEN LPCS.ClaimStatusDescription IS NULL THEN [Status] ELSE LPCS.ClaimStatusDescription END AS StatusNow
	   ,getdate() as LoadDate
--**pyetl2
from #BaseMembers BM, #ClaimDetail CD
	 LEFT JOIN rep0AppianApplicationData.Claim.vw_LookupClaimStatus_All LPCS 
	   ON CD.StatusNow = LPCS.ClaimStatusID
WHERE cd.MemberNumber = bm.MemberKey
	  AND cd.EmployerNum = bm.PlanKey
	  AND CD.MonthEndDate = BM.MonthEndDate



