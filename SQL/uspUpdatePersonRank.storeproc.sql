USE [SubaruAmbassador]
GO
/****** Object:  StoredProcedure [dbo].[uspUpdatePersonRank]    Script Date: 6/8/2022 11:57:17 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==========================================================================================
-- Author:		<kamarthi raghavendra>
-- Description:	<1. this store-proc picks the pending points and adds to user's total points
--				 2. Checks & updates the tier if any users need to be promoted
--				 3. this store-proc also takes are annual reset, two configurable options for annual reset
--					 i.	no demotion -- only the points are reset to min points of the respective tier
--					 2. demotion -- demoted if min point to keep is not reached>
-- ==========================================================================================
ALTER PROCEDURE [dbo].[uspUpdatePersonRank]
	@ModifiedBy NVARCHAR(250),
	@SegmentId INT = 1
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @QueuedStatusId INT, @ProcessedStatusId INT
	DECLARE @CurrentEOYProcessStatusId INT, @EOYPendingStatusId INT,@EOYProcessingStatusId INT, @EOYProcessedStatusId INT
	DECLARE @RankCycleId INT, @NextRankCycleId INT
	DECLARE @CurrentDate DATETIME2, @CurrentCycleStartDate DATETIME2, @CurrentCycleEndDate DATETIME2
	
	DECLARE @ShouldDemoteTierOnReset BIT
	
	SELECT @CurrentDate = GETDATE()




	BEGIN TRY
		INSERT INTO [dbo].[Log]
						([ParentID],[CreateDate],[Priority],[Severity],[Category],[Action],[Controller],[EditorName],[MachineName],[Message],[PayLoad],[Source],[StackTrace],[IpAddress])
						VALUES
						(null,getdate(),'High','Info','WindowsService','[uspUpdatePersonRank]','Not Applicable',@ModifiedBy,'','Begin - PersonRankAutomation Job Run',null,null,null,null)

		BEGIN TRANSACTION
			SELECT @QueuedStatusId = StatusId FROM Status WHERE Name = 'Queued'
			SELECT @ProcessedStatusId = StatusId FROM Status WHERE Name = 'Processed'
			SELECT @EOYPendingStatusId = EOYProcessStatusId from EOYProcessStatus where Name = 'Pending'
			SELECT @EOYProcessingStatusId = EOYProcessStatusId from EOYProcessStatus where Name = 'Processing'
			SELECT @EOYProcessedStatusId = EOYProcessStatusId from EOYProcessStatus where Name = 'Processed'
			
			--Get the current rank cycle id and its start and end date
			SELECT @RankCycleId = RankCycleId, @CurrentCycleStartDate = StartDate, @CurrentCycleEndDate = EndDate, 
			@ShouldDemoteTierOnReset = ShouldDemoteTierOnReset, @CurrentEOYProcessStatusId = EOYProcessStatusId
			from RankCycle where @CurrentDate between StartDate and EndDate and IsActive = 1
			IF(@RankCycleId is NULL)
			BEGIN
				RAISERROR('RankCycleId not found ', 16, 1)
			END

			--Keep the list of persons info who points are going to be updated, required for checking the difference after applying the points
			SELECT PersonId,Tierid, TotalPoints, PromotedInCurrentRankCycle  INTO #TempPersonRankTable
			from PersonRankCurrent WHERE PersonId in (SELECT distinct Personid from PersonActivityPoints WHERE StatusId = @QueuedStatusId and RankCycleId = @RankCycleId)
			
			-- Update person points and their tierid (if promoted)
			UPDATE PRC   
			SET ModifiedBy = @ModifiedBy, ModDate = @CurrentDate, 
			PRC.TotalPoints = PAP.TPoints + PRC.TotalPoints,
			TierId = (SELECT TierId FROM TIER WHERE (PAP.TPoints + PRC.TotalPoints)>=MinPoints AND (PAP.TPoints + PRC.TotalPoints) <=MaxPoints AND SegmentId = @SegmentId)			
			FROM PersonRankCurrent PRC INNER JOIN Person P ON P.PersonId = PRC.PersonId INNER JOIN
			(
				SELECT PersonId, SUM(pp.points) AS TPoints from PersonActivityPoints pp INNER JOIN TierActivity TA  ON pp.TierActivityId = TA.TierActivityId
				INNER JOIN Tier T ON T.TierId = TA.TierId 
				WHERE StatusId = @QueuedStatusId AND T.SegmentId = @SegmentId AND pp.IsActive = 1 AND TA.IsActive = 1 
				--AND COALESCE(pp.OccurrenceDate, pp.CreatedDate) >= @FinancialYrStartDate AND COALESCE(pp.OccurrenceDate, pp.CreatedDate) <= @FinancialYrEndDate 
				AND pp.RankCycleId = @RankCycleId
				group by PersonId
			) AS PAP ON PAP.PersonId = P.PersonId
			

			-- At this point, all pending points are considered, set the status Processed
			UPDATE PersonActivityPoints
			SET StatusId = @ProcessedStatusId, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
			FROM PersonActivityPoints pp INNER JOIN TierActivity TA  ON pp.TierActivityId = TA.TierActivityId INNER JOIN Tier T ON T.TierId = TA.TierId 
			WHERE StatusId = @QueuedStatusId AND T.SegmentId = @SegmentId AND pp.IsActive = 1 AND TA.IsActive = 1 			
			AND pp.RankCycleId = @RankCycleId


			-- set up the table for returning the result back to .net code for further processing
			 DECLARE @Results TABLE
			(
				EmailAddress NVARCHAR(256),
				PersonId INT,
				SegmentId INT,
				PrevTierId INT,
				TierId INT,
				TierName NVARCHAR(256),
				TotalPoints INT,				
				FirstName NVARCHAR(256),
				LastName  NVARCHAR(256),
				CreatedDate datetime2,
				IsPromoted BIT,
				IsPTKReached BIT
			)

			INSERT INTO @Results
			SELECT P.Emailaddress,PRC.PersonId,PRC.SegmentId,TPRC.TierId AS [PrevTierId],PRC.TierId,T.Name AS [TierName],PRC.TotalPoints,P.FirstName,P.LastName, @CurrentDate,1 ,0
			FROM PersonRankCurrent PRC 
			INNER JOIN #TempPersonRankTable TPRC on TPRC.PersonId = PRC.PersonId
			INNER JOIN Person P on P.PersonId = PRC.PersonId
			INNER JOIN Tier T ON T.TierId = PRC.TierId
			WHERE (PRC.TierId  > TPRC.TierId)

			INSERT INTO @Results
			SELECT P.Emailaddress,PRC.PersonId,PRC.SegmentId,TPRC.TierId AS [PrevTierId],PRC.TierId,T.Name AS [TierName],PRC.TotalPoints,P.FirstName,P.LastName, @CurrentDate,0 ,1 
			FROM PersonRankCurrent PRC 
			INNER JOIN #TempPersonRankTable TPRC on TPRC.PersonId = PRC.PersonId
			INNER JOIN Person P on P.PersonId = PRC.PersonId
			INNER JOIN Tier T ON T.TierId = PRC.TierId
			WHERE (PRC.TierId  = TPRC.TierId 
			AND prc.TotalPoints >= T.PointsToKeep AND prc.TierId<>8 AND prc.TierId = 4
			AND PRC.PersonId NOT IN (SELECT PersonId from MilestoneTierPTKReachedTracker where TierId = 4 AND RankCycleId = @RankCycleId)
			)

			-- set the table for milestone notification, 
			INSERT INTO [dbo].[MilestoneTierPTKReachedTracker]
					   ([PersonId],[TierId],[RankCycleId],[Createdate],[Createdby])
			SELECT PersonId, TierId, @RankCycleId, GETDATE(), @ModifiedBy FROM @Results where IsPTKReached = 1


			UPDATE PRC
			SET PromotedInCurrentRankCycle = 1, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
			FROM PersonRankCurrent PRC INNER JOIN #TempPersonRankTable R ON PRC.Personid = R.Personid
			WHERE PRC.PromotedInCurrentRankCycle = 0 and PRC.TierId >  R.TierId 

			
			-- record the history of points
			INSERT INTO PersonRankHistory			
			([PersonId],[SegmentId],[TierId],[TotalPoints],[IsActive],[CreatedDate],[CreatedBy],[ModDate],[ModifiedBy],[RankCycleId],PromotedInCurrentRankCycle)
			SELECT PRC.PersonId,PRC.SegmentId,PRC.TierId,PRC.TotalPoints,1,@CurrentDate,@ModifiedBy,NULL,NULL,@RankCycleId,PRC.PromotedInCurrentRankCycle 
			FROM PersonRankCurrent PRC
			INNER JOIN #TempPersonRankTable TPRC on TPRC.PersonId = PRC.PersonId
			WHERE (TPRC.TierId != PRC.TierId OR TPRC.TotalPoints != PRC.TotalPoints 
			OR PRC.PromotedInCurrentRankCycle != TPRC.PromotedInCurrentRankCycle)

		
			
			-----------------------------------	EOYPR	-----------------------------------		
			

			IF (CAST(@CurrentDate as date) = CAST(@CurrentCycleEndDate as date) 
				AND DATEPART(HOUR, @CurrentDate) = DATEPART(HOUR, @CurrentCycleEndDate) 
		      
			  AND @CurrentEOYProcessStatusId = @EOYPendingStatusId)
				BEGIN
					INSERT INTO [dbo].[Log]
					([ParentID],[CreateDate],[Priority],[Severity],[Category],[Action],[Controller],[EditorName],[MachineName],[Message],[PayLoad],[Source],[StackTrace],[IpAddress])
					VALUES
					(null,getdate(),'High','Info','WindowsService','[uspUpdatePersonRank]','Not Applicable',@ModifiedBy,'','Begin -- End Of Year Reset ShouldDemoteTierOnReset=' + convert(varchar(1),@ShouldDemoteTierOnReset),null,null,null,null)


					UPDATE RankCycle
					SET EOYProcessStatusId = @EOYProcessingStatusId, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
					WHERE RankCycleId = @RankCycleId
						
					SELECT TOP 1 @NextRankCycleId = RankCycleId from RankCycle where StartDate > (select EndDate from RankCycle where RankCycleId = @RankCycleId) order by StartDate						

						-- No demotion, only reset the points
						IF @ShouldDemoteTierOnReset = 0
						BEGIN
						
							
													
							--update ambassador's points to their current tier min points
							UPDATE PRC
							SET totalpoints = T.MinPoints, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
							FROM PersonRankCurrent PRC INNER JOIN Tier T on T.TierId = PRC.TierId
							INNER JOIN Ambassador A on A.PersonId = PRC.PersonId
							INNER JOIN Person P on A.Personid = P.PersonId
							--WHERE A.Statusid = 24 AND A.AIStatusId <> 39 AND A.IsActive = 1 AND P.ProgramId = 1


							---insert history
							INSERT INTO PersonRankHistory			
							([PersonId],[SegmentId],[TierId],[TotalPoints],[IsActive],[CreatedDate],[CreatedBy],[RankCycleId])
							SELECT PRC.Personid,SegmentId,TierId, TotalPoints,PRC.IsActive,@CurrentDate,@ModifiedBy,@NextRankCycleId 
							FROM PersonRankCurrent PRC INNER JOIN Ambassador A on A.PersonId = PRC.PersonId
							INNER JOIN Person P on A.Personid = P.PersonId
							--WHERE A.Statusid = 24 AND A.AIStatusId <> 39 AND A.IsActive = 1 AND P.ProgramId = 1

							UPDATE PersonrankCurrent
							SET PromotedInCurrentRankCycle = 0, moddate= @CurrentDate, ModifiedBy = @ModifiedBy
							WHERE PromotedInCurrentRankCycle = 1

							
						END
						ELSE IF @ShouldDemoteTierOnReset = 1 -- demotion expected, update the tier along with points reset
						BEGIN
							INSERT INTO [dbo].[Log]
							([ParentID],[CreateDate],[Priority],[Severity],[Category],[Action],[Controller],[EditorName],[MachineName],[Message],[PayLoad],[Source],[StackTrace],[IpAddress])
							VALUES
							(null,getdate(),'High','Info','WindowsService','[uspUpdatePersonRank]','Not Applicable',@ModifiedBy,'','Begin -- End Of Year Reset ShouldDemoteTierOnReset=' + convert(varchar(1),@ShouldDemoteTierOnReset),null,null,null,null)

							SELECT PRC.PersonId INTO #Retain
							FROM PersonRankCurrent PRC INNER JOIN Tier T on T.TierId = PRC.TierId
							INNER JOIN Ambassador A on A.PersonId = PRC.PersonId
							INNER JOIN Person P on A.Personid = P.PersonId
							WHERE A.Statusid = 24 AND A.AIStatusId <> 39 AND A.IsActive = 1 AND P.ProgramId = 1
							AND (PRC.PromotedInCurrentRankCycle = 1 OR PRC.TotalPoints >= T.PointsToKeep OR PRC.TierId = 1)

							SELECT PRC.PersonId INTO #Demote
							FROM PersonRankCurrent PRC INNER JOIN Tier T on PRC.TierId = T.TierId 
							INNER JOIN Ambassador A on A.PersonId = PRC.PersonId
							INNER JOIN Person P on A.Personid = P.PersonId
							WHERE A.Statusid = 24 AND A.AIStatusId <> 39 AND A.IsActive = 1 AND P.ProgramId = 1
							AND PRC.PromotedInCurrentRankCycle = 0 AND PRC.TotalPoints < T.PointsToKeep AND PRC.TierId != 1


							UPDATE PRC
							SET totalpoints = T.MinPoints, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
							FROM PersonRankCurrent PRC INNER JOIN Tier T on T.TierId = PRC.TierId
							INNER JOIN #Retain R ON PRC.PersonId = R.PersonId

							UPDATE PRC
							SET  TotalPoints = (select MinPoints from Tier where Tierid = PRC.TierId - 1), TierId = PRC.TierId - 1, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
							FROM PersonRankCurrent PRC INNER JOIN Tier T on PRC.TierId = T.TierId 
							INNER JOIN #Demote D on D.PersonId = PRC.PersonId

							INSERT INTO PersonRankHistory			
							([PersonId],[SegmentId],[TierId],[TotalPoints],[IsActive],[CreatedDate],[CreatedBy],[RankCycleId],[PromotedInCurrentRankCycle])
							SELECT PRC.Personid,SegmentId,TierId, TotalPoints,PRC.IsActive,@CurrentDate,@ModifiedBy,@NextRankCycleId,0
							FROM PersonRankCurrent PRC INNER JOIN Ambassador A on A.PersonId = PRC.PersonId
							INNER JOIN Person P on A.Personid = P.PersonId
							WHERE A.Statusid = 24 AND A.AIStatusId <> 39 AND A.IsActive = 1 AND P.ProgramId = 1


							UPDATE PersonrankCurrent
							SET PromotedInCurrentRankCycle = 0, moddate= @CurrentDate, ModifiedBy = @ModifiedBy
							WHERE PromotedInCurrentRankCycle = 1

						END

					

					UPDATE RankCycle
					SET EOYProcessStatusId = @EOYProcessedStatusId, ModDate = @CurrentDate, ModifiedBy = @ModifiedBy
					WHERE RankCycleId = @RankCycleId
					
					INSERT INTO [dbo].[Log]
					([ParentID],[CreateDate],[Priority],[Severity],[Category],[Action],[Controller],[EditorName],[MachineName],[Message],[PayLoad],[Source],[StackTrace],[IpAddress])
					VALUES
					(null,getdate(),'High','Info','WindowsService','[uspUpdatePersonRank]','Not Applicable',@ModifiedBy,'','END -- End Of Year Reset ShouldDemoteTierOnReset=' + convert(varchar(1),@ShouldDemoteTierOnReset),null,null,null,null)


				END		
						
			-----------------------------------	EOYPR	-----------------------------------	

			INSERT INTO [dbo].[Log]
						([ParentID],[CreateDate],[Priority],[Severity],[Category],[Action],[Controller],[EditorName],[MachineName],[Message],[PayLoad],[Source],[StackTrace],[IpAddress])
						VALUES
						(null,getdate(),'High','Info','WindowsService','[uspUpdatePersonRank]','Not Applicable',@ModifiedBy,'','End - PersonRankAutomation Job Run',null,null,null,null)

			SELECT EmailAddress,PersonId,SegmentId,PrevTierId,TierId,TierName,TotalPoints,FirstName,LastName,CreatedDate,IsPromoted,IsPTKReached FROM @Results
		COMMIT
		
		
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			ROLLBACK
		  
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int, @ErrState int 
		SELECT  @ErrMsg = ERROR_MESSAGE(),@ErrSeverity = ERROR_SEVERITY(),@ErrState = ERROR_STATE()
		INSERT INTO LOG (CreateDate,Priority,Severity,Category,Action,Controller,MachineName,Message)
		VALUES (@CurrentDate,'High','Error','Service','[uspUpdatePersonRank]','NA',@ModifiedBy,@ErrMsg)

		RAISERROR(@ErrMsg, @ErrSeverity, @ErrState)
	END CATCH   
END
