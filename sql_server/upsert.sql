USE not_this_db
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Matt Martin
-- Create date: 2013-11-07
-- Description:	Executes a merge based on the target and source table passed in
/*

	Assumptions	for running this proc
		1) Target table has a primary key
		2) Source Table is a temp table (local "#" or global "##" hash)
		3) Source and Target Table columns are matched based on the name
			- the target and/or the source can have more columns than each other
				- we will only update what is both in target and source
				- we will only insert what is in source and has a corresponding column name in the target
		4) if no schema is supplied for the target table, its assumed to be dbo
*/

-- =============================================
CREATE PROCEDURE [dbo].[sp_upsert] 
	 @target_db	varchar(250)
	,@target_tbl	varchar(500)
	,@target_sch	varchar(250) = 'dbo'
	,@source_tbl	varchar(500)
	,@delt_no_match bit = 0

AS
BEGIN
	SET NOCOUNT ON;
	
	declare
		 @msg varchar(max)
		,@sql nvarchar(max)
		,@tgt_has_last_upd_ts bit
		,@tgt_tbl_full_path varchar(1000) = case 
								when left(@target_tbl,1) = '#' then 'tempdb..'+@target_tbl
								else '['+@target_db+'].['+@target_sch+'].['+@target_tbl+']' 
							end
		,@src_tbl_full_path varchar(1000) = 'tempdb..'+@source_tbl
		,@tgt_object_id int 
		,@src_object_id int
		,@crlf char(2) = char(13)+char(10)
	;

	set @tgt_object_id = object_id(@tgt_tbl_full_path,'U');
	set @src_object_id = object_id(@src_tbl_full_path,'U');

	-- ===================================================================================================================================
	-- Validate Parameters

	-- check target db
	if db_id(@target_db) is null 
		begin
			set @msg = 'Error: Target Database ['+@target_db+'] is not in the server database catalog. Stopping Proc.'
			raiserror(@msg,16,1) with nowait;
			return 16;
		end

	-- check target object
	else if @tgt_object_id is null 
		begin
			set @msg = 'Error: Target Table '+@tgt_tbl_full_path+' does not exist. Stopping Proc.'
			raiserror(@msg,16,1) with nowait;
			return 16;
		end

	-- check source object
	else if @src_object_id is null
		begin
			set @msg = 'Error: Source Table ['+@source_tbl+'] does not exist. Stopping Proc.'
			raiserror(@msg,16,1) with nowait;
			return 16;
		end

	-- check that source is a hash
	else if left(@source_tbl,1) <> '#'
		begin
			set @msg = 'Error: This upsert procedure requires that the source table is a hash/temp table prefixed with "#" or "##". '+@crlf
			+'Source table passed in ['+@source_tbl+'] does not begin with a hash. Stopping Proc.'
			raiserror(@msg,16,1) with nowait;
			return 16;
		end


	-- ======================================================================
	-- Load Table Catalog

	declare @tbl_cat as table (col_nm varchar(250), ord_pos smallint, is_pk bit, pk_ord_pos smallint, exists_in_src bit);

	set @sql = 
		'
			select c.name, c.column_id
				,case when ic.index_column_id is not null then 1 else 0 end as is_pk
				,ic.key_ordinal as pk_ord_pos
				,case when src.name is not null then 1 else 0 end as exists_in_src
			from '+@target_db+'.sys.columns as c with (nolock)
				left join '+@target_db+'.sys.indexes as i with (nolock)
					on c.object_id = i.object_id and i.is_primary_key = 1 
				left join '+@target_db+'.sys.index_columns as ic with (nolock)
					on i.object_id = ic.object_id and i.index_id = ic.index_id and c.column_id = ic.column_id
				left join (
						select name from tempdb.sys.columns with (nolock) where object_id = '+cast(@src_object_id as varchar(50))+'
					) as src
						on c.name = src.name
			where c.object_id = '+cast(@tgt_object_id as varchar(50))+' and c.is_identity = 0
		'
	;

	insert @tbl_cat (col_nm, ord_pos, is_pk, pk_ord_pos, exists_in_src)
	exec(@sql);
	
	-- ---------------------------------------------------------------------------------------
	-- Check that a primary key exists in the target

	if not exists(select 1 from @tbl_cat where is_pk = 1)
		begin
			set @msg = 
				 'Error: Target Table '+@tgt_tbl_full_path+' does not contain a primary key.'+@crlf
				+'This upsert proc requires a primary key on the target table to ensure how to join the target and source tables correctly. Stopping Proc.'
			raiserror(@msg,16,1) with nowait;

			return 16;
		end

	-- -----------------------------------------------------------------------------------------------------------------------------
	-- End Param validation     declare 
		
		 @join_txt nvarchar(max)
		,@matched_txt nvarchar(max)
		,@unmatched_insert_txt varchar(max)
		,@unmatched_values_txt varchar(max)
		,@last_upd_ts nvarchar(30)
		,@existing_ci_nm nvarchar(250)
	;

	/*
		Merge/Upsert statements have 3 basic parts:
		1 - The target and source table specification with join
		2 - the matched clause (update part)
		3 - The unmatched clause (insert part)

		The proc below assembles a merge statement concatenating these three parts together	and then executes said merge statement


	*/

	-- -------------------------------------------------------------------------------------------------
	-- Check if the target has a last_upd_ts column

	if exists(select 1 from @tbl_cat where col_nm = 'last_upd_ts') 
		begin
			set @tgt_has_last_upd_ts = 1
		end
	else
		begin
			set @tgt_has_last_upd_ts = 0
		end
	;
	-- ===================================================================================================================
	-- ===================================================================================================================
	-- [2] Build Out Merge SQL Text
		-- consists of 3 parts
			-- - i. Join Text
			-- - ii. Match Text
			-- - iii. Not Matched Text
	
	-- this spacer helps for formatting the merge text getting assembled
	declare @spacer varchar(50) = @crlf+'						';

	-- ===================================================================================================================
	-- [2i]. Join Text 

	select @join_txt = isnull(@join_txt,'')
		-- add in a break in the text to make it more readable
		+case when ROW_NUMBER() over(order by pk_ord_pos) % 3 = 0 then @spacer else '' end
		+' and tgt.['+col_nm+'] = src.['+col_nm+']' 
	from @tbl_cat
	where is_pk = 1
	;

	-- trim off the first "and "
	set @join_txt = right(@join_txt,len(@join_txt) - 5);

	-- ===================================================================================================================
	-- [2ii]. Matched Text

	select @matched_txt = 
		  isnull(@matched_txt,'') 
		-- add line breaker every three recs for merge text format
		+ case when ROW_NUMBER() over(order by t.ord_pos) % 3 = 0 then @spacer else '' end
		+', tgt.['+t.col_nm+'] = src.['+t.col_nm+']'
	from @tbl_cat as t
	where t.is_pk <> 1 and t.col_nm <> 'last_upd_ts' and exists_in_src = 1
	order by t.ord_pos
	;

	-- trim off the leading spaces & breaker
	set @matched_txt = right(@matched_txt,len(@matched_txt) -2);
	
	-- if a last_upd_ts column exists, tack it on the end of the update
	if @tgt_has_last_upd_ts = 1 begin

		set @matched_txt = @matched_txt + ', tgt.[last_upd_ts] = @now';
	end

	-- ===================================================================================================================
	-- [2iii]. Not Matched Text
	
	select @unmatched_insert_txt = 
		isnull(@unmatched_insert_txt,'')
		-- add line breaker every three recs for merge text format
		+ case when ROW_NUMBER() over(order by t.ord_pos) % 6 = 0 then @spacer else '' end
		+ ', ['+t.col_nm+']'
	from @tbl_cat as t
	where t.col_nm <> 'last_upd_ts' and exists_in_src = 1
	order by t.ord_pos
	;

	-- trim off the leading spaces & breaker
	set @unmatched_insert_txt = right(@unmatched_insert_txt,len(@unmatched_insert_txt) - 2);

	set @unmatched_values_txt = replace(@unmatched_insert_txt, '[','src.[');

	-- add last_upd_ts if exists
	if @tgt_has_last_upd_ts = 1 
		begin

			set @unmatched_insert_txt = @unmatched_insert_txt + ', [last_upd_ts]';
			set @unmatched_values_txt = @unmatched_values_txt + ', @now';

		end

	
	-- ====================================================================================================================
	-- [3] Optimize the temp table to prepare for Upsert

		-- [a] Create unique non-clustered covering index

	declare @cover_clause varchar(max) = '('+stuff(
		(
			select ', ['+t.col_nm+']'
			from @tbl_cat as t
			where t.col_nm <> 'last_upd_ts'
				and t.is_pk <> 1
				and exists_in_src = 1
			order by t.ord_pos 
		for xml path ('')),1,2,'')+')'
			
	;

	set @sql = 'create unique nonclustered index ['+(select cast(newid() as varchar(50)))+']  on ['+@source_tbl+']'	
				+' ('+stuff((select ', ['+col_nm+']' from @tbl_cat where is_pk = 1 order by pk_ord_pos for xml path ('')),1,2,'')+')';
	;
	
	-- update covering predicate
	set @sql = @sql + ' INCLUDE '+@cover_clause;

	begin try
		exec(@sql);
	end try
	begin catch

		-- If the error was a dup...load it into a global hash and email out the recs

		if charindex('The CREATE UNIQUE INDEX statement terminated because a duplicate key', error_message()) > 0
			begin

				declare @dups_sql varchar(max)

				declare @tmp_on_clause varchar(max), @gtt varchar(250), @tmp_keys varchar(max);

				set @tmp_keys = stuff((select ', ['+col_nm+']' from @tbl_cat where is_pk = 1 order by pk_ord_pos for xml path ('')),1,2,'')

				set @tmp_on_clause = 'on '+stuff((select 'and tmp.['+col_nm+'] = dups.['+col_nm+'] ' from @tbl_cat where is_pk = 1 order by pk_ord_pos for xml path ('')),1,3,'')

				set @gtt = '##'+cast(replace(newid(),'-','_') as varchar(250));


				-- isolate dups to global temp
				set @dups_sql = 
					'
						select tmp.*
						into ['+@gtt+']
						from '+@source_tbl+' as tmp
							inner join (
								select '+@tmp_keys+'
								from '+@source_tbl+'
								group by '+@tmp_keys+'
								having count(*) >=2
							) as dups
								'+@tmp_on_clause
				;

				exec(@dups_sql);

				-- just give the top 1000 recs
				set @dups_sql = 'set nocount on; select top 1000 * from ['+@gtt+']';

				set @msg = 
					 'Note: During proc sp_upsert, duplicate records were detected, and the proc failed.<br>'
					+'Attached is a list of duplicate records found during the upsert attempt.<br>'
					+'Notes on usp_upsert proc:<br><br>'
					+'<ul>'
						+'<li>Source Table: '+@source_tbl+'</li>'
						+'<li>Target Table: '+@tgt_tbl_full_path+'</li>'
						+'<li>Target Key Column(s): '+@tmp_keys+'</li>'
					+'</ul>'
				;
					
				-- SEND EMAIL
				exec msdb.dbo.sp_send_dbmail
					 @recipients = 'example@yourBiz.com'
					,@subject = 'Duplicates found during sp_upsert'
					,@body = @msg
					,@query = @dups_sql
					,@query_attachment_filename = 'dups.csv'
					,@attach_query_result_as_file = 1
					,@query_result_separator = '	'
					,@query_result_header = 1
					,@query_result_width = 32767
					,@query_result_no_padding = 1
					,@exclude_query_output = 1
					,@body_format = 'HTML'
				;

				-- drop global hash once complete
				if object_id('tempdb..'+@gtt) is not null exec('drop table '+@gtt);

			end

		-- throw it
		set @msg = 'Error attempting to create unique index on table ['+@source_tbl+'] during the usp_run_upsert proc'+@crlf
			+'Error Message: '+error_message()
		raiserror(@msg,16,16);
		return 16
	end catch



	-- =====================================================================================================================
	-- [4] Assemble Merge command

	set @sql = 
		'
			'+ case when @tgt_has_last_upd_ts = 1 then 'DECLARE @now DATETIME2 = getdate();' else '' end+'
			MERGE ['+@target_db+'].['+@target_sch+'].['+@target_tbl+'] WITH (TABLOCK) AS TGT
				USING ['+@source_tbl+'] AS SRC WITH (TABLOCK)
					ON 
						     '+@join_txt+'
				WHEN MATCHED THEN UPDATE 
					SET  
						  '+@matched_txt+'
				WHEN NOT MATCHED BY TARGET THEN
					INSERT (
						  '+@unmatched_insert_txt+')
					VALUES (
						  '+@unmatched_values_txt+')
				'+case when @delt_no_match = 1 then 'WHEN NOT MATCHED BY SOURCE THEN DELETE' else '' end +'
			;
			
		'
	;

	-- run it
	begin try
		--print @sql
		
		exec(@sql);

	end try
	begin catch
		
		-- note: we are breaking the error read-out into two parts because the raiserror msg only supports 2048 characters

		set @msg = ''
		set @msg = @msg + '<<< ERROR RUNNING SP_UPSERT >>>'+@crlf;
		set @msg = @msg + 'Below is the error readout and the merge statement that forced the error. Please review.'+@crlf;
		set @msg = @msg + 'error message -> '+ error_message()+@crlf;
		set @msg = @msg + 'error line -> '+ cast(error_line() as varchar(5))+@crlf;
		set @msg = @msg + '========================================================================================'+@crlf;

		raiserror(@msg,16,16);

		set @msg = ''
		set @msg = @msg + 'Merge statement that forced error.'+@crlf;
		set @msg = @msg + '--********************************--'+@crlf;
		set @msg = @msg + @sql;

		raiserror(@msg, 16,16);

		-- get out of dodge
		return 16

	end catch

END