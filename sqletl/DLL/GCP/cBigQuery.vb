Imports System.IO
Imports System.Data.SqlClient
Imports System.Threading
Imports Google.Apis.Bigquery.v2
Imports Google.Apis.Auth.OAuth2
Imports Google.Apis.Services
Imports Google.Apis.Bigquery.v2.Data

'Required References:
'[1] google.apis
'[2] google.apis.auth
'[3] google.apis.auth.platformservices
'[4] google.apis.bigquery.v2
'[5] google.apis.core
'[6] google.apis.platformservices
'[7] newtonsoft.json


Friend Class cBigQuery

    Shared retry_limit As Integer = 3

    Shared bq_qry_timeout_ms As Integer = 21600000 '6 hours

    Shared bq_ref_job_id As String = ""

    Shared Sub main(ByRef err_msg As String)
        Select Case EDIS.get_edis_pval("sub_task").ToLower
            Case "import_data"
                log_edis_event("info", "BQ", "Main import data")
                import_data(err_msg)
            Case "crt_tbl_from_qry"
                bq_crt_tbl_from_qry(err_msg)
            Case "export_data"
                export_bq_data(err_msg)
            Case "drop_table"
                bq_drop_table(err_msg)
            Case "append_data"
                bq_append_data(err_msg)
            Case "sql_cmd"
                bq_run_sql_cmd(err_msg)

        End Select

        If Not String.IsNullOrWhiteSpace(err_msg) And Not String.IsNullOrWhiteSpace(bq_ref_job_id) Then
            err_msg += vbCrLf + "BigQuery Job Reference ID: {" + bq_ref_job_id + "}"
        End If

    End Sub

    Shared Function does_bq_tbl_exist(BQ_Service As BigqueryService, project_id As String, dataset_nm As String, tbl_nm As String, ByRef err_msg As String) As Boolean
        Try
            Dim res As Boolean = False

            Dim dataset_found As Boolean = False

            Dim dataset_list = BQ_Service.Datasets.List(project_id).Execute().Datasets

            Dim act_dataset_nm As String

            For Each dataset In dataset_list

                If dataset.Id.ToString.Split(":")(1).ToLower = dataset_nm.ToLower Then

                    dataset_found = True
                    act_dataset_nm = dataset.Id.ToString.Split(":")(1)

                    Dim tableList = BQ_Service.Tables.List(dataset.Id.ToString.Split(":")(0), dataset.Id.ToString.Split(":")(1)).Execute().Tables

                    If tableList IsNot Nothing Then
                        For Each tbl In tableList

                            Dim tbl_nm_full As String = tbl.Id.ToString.Split(":")(1)
                            Dim curr_tbl_short As String = tbl_nm_full.Split(".")(1)

                            If curr_tbl_short.ToLower() = tbl_nm.ToLower Then
                                log_edis_event("info", "BigQuery find Table", String.Format("Table {0}, in dataset {1} found", curr_tbl_short, act_dataset_nm))
                                res = True
                                Exit For
                            End If
                        Next
                    End If


                    If dataset_found Then Exit For

                End If

            Next dataset

            Return res

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Function

    Shared Function get_bq_tbl_col_list(BQ_Service As BigqueryService, project_nm As String, dataset_nm As String, tbl_nm As String, ByRef err_msg As String) As List(Of String)

        Try

            Dim cols As New List(Of String)

            Dim bq_fields = BQ_Service.Tables.Get(project_nm, dataset_nm, tbl_nm).Execute.Schema.Fields

            For Each bq_field In bq_fields
                Dim col_nm As String = bq_field.Name.ToString
                Dim data_type As String = bq_field.Type.ToString()
                Dim null_disposition As String = bq_field.Mode.ToString()

                Dim is_nullable As Boolean = False
                If null_disposition = "NULLABLE" Then
                    is_nullable = True
                End If

                cols.Add(col_nm)

            Next

            Return cols

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Function

    'get case sensitive dataset name (datasets are CASE SENSITIVE)
    Shared Function get_cs_dataset_nm(bq_svc As BigqueryService, project_id As String, dataset_nm As String, ByRef err_msg As String) As String
        Dim res As String = ""
        Try
            Dim dataset_list = bq_svc.Datasets.List(project_id).Execute().Datasets
            For Each dataset In dataset_list

                If dataset.Id.ToString.Split(":")(1).ToLower = dataset_nm.ToLower Then
                    res = dataset.Id.ToString.Split(":")(1)
                    Exit For
                End If
            Next

            If res = "" Then
                err_msg = String.Format("Error finding dataset {0} in project {1}", dataset_nm, project_id)
                Exit Function
            End If

            Return res
        Catch ex As Exception
            err_msg = "Error retrieving case-sensitive dataset name: " + ex.Message.ToString()
        End Try

    End Function

    Shared Function drop_bq_tbl_internal(bq_svc As BigqueryService, project_id As String, dataset_nm As String, tbl_nm As String, ByRef err_msg As String) As Boolean

        Dim res As Boolean = False

        Try

            Dim dataset_found As Boolean = False

            Dim dataset_list = bq_svc.Datasets.List(project_id).Execute().Datasets

            Dim act_dataset_nm As String

            For Each dataset In dataset_list

                Dim curr_dataset As String = dataset.Id.ToString.Split(":")(1)

                log_edis_event("info", "Bigquery drop table", String.Format("checking dataset {0}", curr_dataset))

                'edit 3/27/2018...datasets are case sensitive
                If Trim(curr_dataset) = Trim(dataset_nm) Then
                    'If curr_dataset.ToLower = dataset_nm.ToLower Then

                    dataset_found = True
                    act_dataset_nm = curr_dataset

                    Dim tableList = bq_svc.Tables.List(dataset.Id.ToString.Split(":")(0), dataset.Id.ToString.Split(":")(1)).Execute().Tables

                    If tableList IsNot Nothing Then
                        log_edis_event("info", "BigQuery Drop", "looping through tables")
                        For Each tbl In tableList

                            Dim tbl_nm_full As String = tbl.Id.ToString.Split(":")(1)
                            log_edis_event("info", "BigQuery Drop", "current table full name " + tbl_nm_full)
                            Dim curr_tbl_short As String = tbl_nm_full.Split(".")(1)

                            log_edis_event("info", "BigQuery Drop", "current table short name " + curr_tbl_short)

                            'edit 3/27/2018...tables are case-sensitive in BQ
                            If Trim(curr_tbl_short) = Trim(tbl_nm) Then
                                'If curr_tbl_short.ToLower() = tbl_nm.ToLower Then
                                log_edis_event("info", "BigQuery Drop Table", String.Format("Dropping Table {0}, in dataset {1}", curr_tbl_short, act_dataset_nm))
                                bq_svc.Tables.Delete(project_id, act_dataset_nm, curr_tbl_short).Execute()
                                log_edis_event("info", "BigQuery Drop Table", String.Format("Table {0}, in dataset {1} dropped", curr_tbl_short, act_dataset_nm))
                                'Console.WriteLine("deleted table {0}", curr_tbl_short)
                                res = True
                                Exit For
                            End If
                        Next
                    End If


                    If dataset_found Then Exit For

                End If

            Next dataset

            Return res

        Catch ex As Exception
            err_msg = ex.Message.ToString()
            Return False
        End Try

    End Function

    Shared Sub bq_run_sql_cmd(ByRef err_msg As String)

        Dim err_prefix As String = "BigQuery SQL Command Error: "

        Dim retry_cnt As Integer = 0
        Dim cred As GoogleCredential


        Try

bq_start:
            Dim creds_str As String = EDIS.get_edis_pval("client_secrets")
            Dim project_nm As String = EDIS.get_edis_pval("project_nm")
            Dim project_id As String = EDIS.get_edis_pval("project_id")
            Dim dataset_nm As String = EDIS.get_edis_pval("dataset_nm")
            Dim sql_cmd As String = EDIS.get_edis_pval("sql_cmd")


            Dim start As DateTime = Now

            Console.WriteLine("started at {0}", start.ToShortDateString + " " + start.ToShortTimeString)



            '--------------------------------------------------------------------------------------
            'Load Credentials
            cred = load_bq_svc_creds(creds_str, err_msg)
            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BigQuery Credentials", "credentials loaded")
            '--------------------------------------------------------------------------------------
            'Create Service

            Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)

            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BigQuery Service", "service created")

            Dim cmds() As String = sql_cmd.Split(New String() {";"}, StringSplitOptions.RemoveEmptyEntries)

            Dim cmd_cnt As Integer = 1
            For Each cmd In cmds

                'dont run if the command is empty
                If String.IsNullOrWhiteSpace(cmd) Then GoTo skip_sql_cmd_run

                Dim job_id As Guid = Guid.NewGuid()

                'tag for error handling
                bq_ref_job_id = job_id.ToString

                Dim jR As JobsResource = BQ_Service.Jobs()
                Dim thisJob As Job = New Job()

                Dim jRef As New JobReference
                jRef.JobId = job_id.ToString
                jRef.ProjectId = project_id

                thisJob.JobReference = jRef


                thisJob.Configuration = New JobConfiguration() With {.Query = New JobConfigurationQuery() With {
                    .DefaultDataset = New DatasetReference() With {.ProjectId = project_id, .DatasetId = dataset_nm},
                    .Query = cmd,
                    .UseLegacySql = False
                }
                }

                Dim res = jR.Insert(thisJob, project_id).Execute()

                'wait for finish
                While True
                    Dim poll_rsp = BQ_Service.Jobs.Get(project_id, job_id.ToString).Execute()
                    If poll_rsp.Status.State.Equals("DONE") Then

                        'get job errors
                        If poll_rsp.Status.ErrorResult IsNot Nothing Then
                            'err_msg = poll_rsp.Status.ErrorResult.Message.ToString()
                            Dim err_rsn = poll_rsp.Status.ErrorResult.Reason
                            Dim err_loc = poll_rsp.Status.ErrorResult.Location


                            For Each eMsg In poll_rsp.Status.Errors
                                err_msg += eMsg.Message.ToString()
                                'Console.WriteLine(eMsg.Message.ToString())
                            Next

                            'add cmd cnt
                            err_msg += vbCrLf + "Error occured on command sequence " + cmd_cnt.ToString()

                        End If
                        Exit While
                    End If
                    Thread.Sleep(2000)
                End While

                cmd_cnt += 1

skip_sql_cmd_run:

            Next


        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub

    Shared Sub bq_append_data(ByRef err_msg As String)


        Dim err_prefix As String = "BigQuery Append Data Error: "

        Dim retry_cnt As Integer = 0
        Dim cred As GoogleCredential

        Try

bq_start:

            Dim creds_str As String = EDIS.get_edis_pval("client_secrets")
            Dim project_nm As String = EDIS.get_edis_pval("project_nm")
            Dim project_id As String = EDIS.get_edis_pval("project_id")

            Dim dataset_nm As String = EDIS.get_edis_pval("dataset_nm")
            Dim dest_tbl_nm As String = EDIS.get_edis_pval("dest_tbl_nm")
            Dim src_qry As String = EDIS.get_edis_pval("src_qry")

            Dim start As DateTime = Now

            Console.WriteLine("started at {0}", start.ToShortDateString + " " + start.ToShortTimeString)

            Dim job_id As Guid = Guid.NewGuid()

            bq_ref_job_id = job_id.ToString

            '--------------------------------------------------------------------------------------
            'Load Credentials
            cred = load_bq_svc_creds(creds_str, err_msg)
            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BQ Append Data Task", "credentials loaded")
            '--------------------------------------------------------------------------------------
            'Create Service

            Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)

            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BQ Append Data Task", "service created")

            Dim jR As JobsResource = BQ_Service.Jobs()
            Dim thisJob As Job = New Job()



            Dim jRef As New JobReference
            jRef.JobId = job_id.ToString
            jRef.ProjectId = project_id

            thisJob.JobReference = jRef

            Dim schema_ops As IList(Of String) = New List(Of String)



            schema_ops.Add("ALLOW_FIELD_RELAXATION") 'handles going from required to null
            schema_ops.Add("ALLOW_FIELD_ADDITION") 'handles adding more fields

            'update the dataset name for case sensitivity
            dataset_nm = get_cs_dataset_nm(BQ_Service, project_id, dataset_nm, err_msg)
            If err_msg <> "" Then Exit Sub

            thisJob.Configuration = New JobConfiguration() With {.Query = New JobConfigurationQuery() With {
                .AllowLargeResults = True,
                .CreateDisposition = "CREATE_NEVER",
                .DefaultDataset = New DatasetReference() With {.ProjectId = project_id, .DatasetId = dataset_nm},
                .DestinationTable = New TableReference() With {.ProjectId = project_id, .DatasetId = dataset_nm, .TableId = dest_tbl_nm},
                .Query = src_qry,
                .WriteDisposition = "WRITE_APPEND",
                .UseLegacySql = False,
                .SchemaUpdateOptions = schema_ops
            }
            }

            Dim res = jR.Insert(thisJob, project_id).Execute()

            'wait for finish
            While True
                Dim poll_rsp = BQ_Service.Jobs.Get(project_id, job_id.ToString).Execute()
                If poll_rsp.Status.State.Equals("DONE") Then

                    'get job errors
                    If poll_rsp.Status.ErrorResult IsNot Nothing Then
                        'err_msg = poll_rsp.Status.ErrorResult.Message.ToString()
                        Dim err_rsn = poll_rsp.Status.ErrorResult.Reason
                        Dim err_loc = poll_rsp.Status.ErrorResult.Location


                        For Each eMsg In poll_rsp.Status.Errors
                            err_msg += eMsg.Message.ToString()
                            'Console.WriteLine(eMsg.Message.ToString())
                        Next

                    End If
                    Exit While
                End If
                Thread.Sleep(2000)
            End While

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString()

            check_error(err_msg, retry_cnt)
            If retry_cnt <= retry_limit Then
                GoTo bq_start
            Else
                Exit Sub
            End If
        End Try

    End Sub

    Shared Sub export_bq_data(ByRef err_msg As String)

        Dim err_prefix As String = "BigQuery Export Error: "

        Dim file_path As String

        Dim retry_cnt As Integer = 0
        Dim cred As GoogleCredential

        Try

            Dim delim As String = "ASCII::8"
            Dim work_dir As String = EDIS.get_edis_pval("work_dir")
            Dim creds_str As String = EDIS.get_edis_pval("client_secrets")
            Dim project_nm As String = EDIS.get_edis_pval("project_nm")
            Dim project_id As String = EDIS.get_edis_pval("project_id")

            Dim dataset_nm As String = EDIS.get_edis_pval("dataset_nm")

            Dim dest_tbl_nm As String = EDIS.get_edis_pval("dest_tbl_nm")
            ' Dim crt_dest_tbl As Boolean = True
            '  Dim crt_dest_tbl As Boolean = CBool(EDIS.get_edis_pval("crt_dest_tbl"))
            Dim truncate_existing As Boolean = CBool(EDIS.get_edis_pval("truncate_existing"))
            Dim drop_existing As Boolean = CBool(EDIS.get_edis_pval("drop_existing"))
            Dim src_qry As String = EDIS.get_edis_pval("src_qry")
            Dim text_qual As String = "" ' EDIS.get_edis_pval("text_qual")
            Dim mssql_inst As String = EDIS._mssql_inst.ToString
            Dim merge_tgt As Boolean = CBool(EDIS.get_edis_pval("merge_dest_tbl"))
            Dim tgt_key_cols As String = EDIS.get_edis_pval("dest_tbl_key_cols")
            Dim src_sys_nm As String = EDIS.get_edis_pval("src_sys_nm")
            Dim src_platform As String = EDIS.get_edis_pval("src_platform").ToUpper
            Dim src_cn_provider As String = EDIS.get_edis_pval("src_cn_provider").ToUpper
            Dim src_cn_sub_provider As String = EDIS.get_edis_pval("src_cn_sub_provider").ToUpper
            Dim src_cn_str As String = EDIS.get_edis_pval("src_cn_str")

            'ado.net handler
            'If src_cn_provider = "ADO.NET" Then
            '    src_cn_provider = src_cn_provider + "_" + src_cn_sub_provider
            'End If


            'handle issue if users says drop existing and merge so that the target doesnt get dropped by accident
            If drop_existing And merge_tgt Then
                drop_existing = False
            End If


            Dim start As DateTime = Now

            '---------------------------------------------------------------------------------------------------------------------
            'Run Data Transfer to CSV

            If Right(work_dir, 1) <> "\" Then
                work_dir += "\"
            End If


            '----------------------------------------------------------------------------------------------------------------------
            'If the source is SQL Server, convert the provider to native client so that things like dates and timestamps are translated correctly

            'NOTE: We are only doing this here in the bigquery module so we dont affect other data transfer tasks

            ' SQLOLEDB in a data flow inteprets dates and datetime2 as text

            Dim sql_server_version As Integer
            If src_platform.ToUpper = "MSSQL" And (src_cn_provider.ToUpper = "OLEDB" Or src_cn_sub_provider.ToUpper = "OLEDB") Then

                Dim src_sql_instance As String = cDT_Tasks.get_cn_str_prop(src_cn_str, "DATA_SOURCE", err_msg)
                If err_msg <> "" Then Exit Sub

                Dim sql_user_id As String = cDT_Tasks.get_cn_str_prop(src_cn_str, "USER_ID", err_msg)
                If err_msg <> "" Then Exit Sub
                Dim sql_password As String = cDT_Tasks.get_cn_str_prop(src_cn_str, "PASSWORD", err_msg)
                If err_msg <> "" Then Exit Sub

                sql_server_version = cDT_Tasks.get_mssql_version(src_sql_instance, sql_user_id, sql_password, err_msg)
                If err_msg <> "" Then Exit Sub

                Select Case True
                            '2012 and above uses client 11
                    Case sql_server_version >= 110
                        src_cn_str = src_cn_str.Replace("SQLOLEDB", "SQLNCLI11")
                                '2008 uses native client 10
                    Case sql_server_version = 100
                        src_cn_str = src_cn_str.Replace("SQLOLEDB", "SQLNCLI10")
                End Select

                'Select Case sql_server_version
                '            '2012 and above uses client 11
                '    Case 110, 120, 130
                '        src_cn_str = src_cn_str.Replace("SQLOLEDB", "SQLNCLI11")
                '                '2008 uses native client 10
                '    Case 100
                '        src_cn_str = src_cn_str.Replace("SQLOLEDB", "SQLNCLI10")
                'End Select


            End If

            file_path = work_dir + Guid.NewGuid().ToString + ".csv"
            Dim is_unicode As Boolean = True 'send as unicode to handle weird text
            Dim file_code_page As String = "UTF8"
            cData_Transfer_Shell.run_data_transfer_db_to_ff_out(src_sys_nm, src_cn_str, src_cn_provider, src_cn_sub_provider, src_platform, src_qry,
                                                                file_path, delim, "{CR}{LF}", text_qual, False, True, False, file_code_page, err_msg)

            If err_msg <> "" Then
                Exit Sub
            End If


            '---------------------------------------------------------------------------------------------------------------------
            'Load CSV to BigQuery

            'Console.WriteLine("started at {0}", start.ToShortDateString + " " + start.ToShortTimeString)

bq_start:

            Dim job_id As Guid = Guid.NewGuid()
            bq_ref_job_id = job_id.ToString


            '--------------------------------------------------------------------------------------
            'Load Credentials
            cred = load_bq_svc_creds(creds_str, err_msg)
            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    If File.Exists(file_path) Then
                        File.Delete(file_path)
                    End If
                    Exit Sub
                End If
            End If

            log_edis_event("info", "select BQ import task", "credentials loaded")
            '--------------------------------------------------------------------------------------
            'Create Service

            Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)

            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    If File.Exists(file_path) Then
                        File.Delete(file_path)
                    End If
                    Exit Sub
                End If
            End If

            log_edis_event("info", "select BQ import task", "service created")

            Dim intermediate_tbl As String = "EDIS_temp_" + Guid.NewGuid().ToString.Replace("-", "_")

            'update dataset name for case sensitivity
            dataset_nm = get_cs_dataset_nm(BQ_Service, project_id, dataset_nm, err_msg)
            If err_msg <> "" Then Exit Sub


            Dim tgt_tbl As New TableReference
            tgt_tbl.ProjectId = project_id
            tgt_tbl.DatasetId = dataset_nm

            'if this is a first time merge...then check if the table exists..if not, create it
            Dim merge_first_time As Boolean

            Dim does_dest_tbl_exist As Boolean = does_bq_tbl_exist(BQ_Service, project_id, dataset_nm, dest_tbl_nm, err_msg)
            If err_msg <> "" Then Exit Sub


            If merge_tgt And does_dest_tbl_exist Then
                tgt_tbl.TableId = intermediate_tbl
            Else
                tgt_tbl.TableId = dest_tbl_nm
            End If



            Dim jb As New Job
            Dim jcf As New JobConfiguration
            Dim cfLoad As New JobConfigurationLoad



            'drop existing table?
            If drop_existing Then
                drop_bq_tbl_internal(BQ_Service, project_id, dataset_nm, dest_tbl_nm, err_msg)
                If err_msg <> "" Then
                    Exit Sub
                End If
            End If

            '-------------------------------------------------------------------------------------------------------------
            'get the source query schema

            Dim tb_schema As New TableSchema

            'Dim bq_cols As New List(Of bq_export_col)
            Dim cols As IList(Of TableFieldSchema) = New List(Of TableFieldSchema)

            For Each itm As EDIS.c_data_transfer_src_meta In EDIS.data_transfer_src_metadata

                Dim fl As New TableFieldSchema
                fl.Name = itm.col_nm
                fl.Mode = "NULLABLE"

                Select Case itm.dts_data_type.ToUpper
                    Case "DT_I1", "DT_I2", "DT_I4", "DT_UI1", "DT_UI1", "DT_UI2", "DT_UI4", "DT_I8"
                        fl.Type = "INTEGER"
                    Case "DT_DBDATE"
                        fl.Type = "DATE"
                    Case "DT_BOOL"
                        fl.Type = "BOOLEAN"
                    Case "DT_DBTIMESTAMP", "DT_DBTIMESTAMP2"
                        fl.Type = "DATETIME"
                    Case "DT_DATE", "DT_DBTIMESTAMPOFFSET"
                        fl.Type = "TIMESTAMP"
                    Case "DT_R4", "DT_R8", "DT_CY", "DT_NUMERIC"
                        fl.Type = "FLOAT"
                    Case "DT_WSTR", "DT_NTEXT", "DT_STR", "DT_TEXT"
                        fl.Type = "STRING"
                    Case Else
                        fl.Type = "STRING"
                End Select

                cols.Add(fl)
            Next

            log_edis_event("info", "Bigquery export", "meta data assigned from DFT")

            'before when it was looking at MSSQL only
            'Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")

            '    cn.Open()

            '    Dim cmd As New SqlCommand

            '    cmd.Connection = cn
            '    cmd.Parameters.AddWithValue("@tsql", src_qry)
            '    cmd.CommandType = CommandType.StoredProcedure
            '    cmd.CommandText = "sys.sp_describe_first_result_set"


            '    Using reader = cmd.ExecuteReader()
            '        While reader.Read()

            '            Dim col_nm As String = reader("name").ToString
            '            Dim sql_data_type As String = reader("system_type_name").ToString.ToLower

            '            If sql_data_type Like "datetime2*" Then
            '                sql_data_type = "datetime2"
            '            ElseIf sql_data_type Like "datetimeoffset*" Then
            '                sql_data_type = "datetimeoffset"
            '            ElseIf sql_data_type Like "datetime*" Then
            '                sql_data_type = "datetime"
            '            End If

            '            Dim ord_pos As Integer = reader("column_ordinal")
            '            Dim null_mode As String
            '            If reader("is_nullable") = "1" Then
            '                null_mode = "NULLABLE"
            '            Else
            '                null_mode = "REQUIRED"
            '            End If

            '            Dim bq_data_type As String
            '            Select Case sql_data_type
            '                Case "int", "bigint", "smallint", "tinyint"
            '                    bq_data_type = "INTEGER"
            '                Case "date"
            '                    bq_data_type = "DATE"
            '                'bq_data_type = "DATETIME"
            '                Case "datetime", "datetime2", "timestamp"
            '                    'bq_data_type = "DATETIME"
            '                    bq_data_type = "TIMESTAMP"
            '                Case "datetimeoffset"
            '                    bq_data_type = "TIMESTAMP"
            '                Case "bit"
            '                    bq_data_type = "BOOLEAN"
            '                Case "decimal", "numeric", "money", "float", "real"
            '                    bq_data_type = "FLOAT"
            '                Case Else
            '                    bq_data_type = "STRING"
            '            End Select

            '            Dim fl As New TableFieldSchema
            '            fl.Name = col_nm
            '            fl.Type = bq_data_type
            '            fl.Mode = null_mode


            '            cols.Add(fl)


            '        End While


            '    End Using

            'End Using


            tb_schema.Fields = cols


            cfLoad.Schema = tb_schema
            cfLoad.DestinationTable = tgt_tbl
            If file_code_page = "UTF8" Then
                cfLoad.Encoding = "UTF-8"
            Else
                cfLoad.Encoding = "ISO-8859-1"
            End If


            'new default behavior is create if needed
            cfLoad.CreateDisposition = "CREATE_IF_NEEDED"

            'If crt_dest_tbl Or merge_tgt Then
            '    cfLoad.CreateDisposition = "CREATE_IF_NEEDED"
            'Else
            '    cfLoad.CreateDisposition = "CREATE_NEVER"
            'End If

            If truncate_existing Then
                cfLoad.WriteDisposition = "WRITE_TRUNCATE"
            Else
                cfLoad.WriteDisposition = "WRITE_APPEND"
            End If

            cfLoad.FieldDelimiter = vbBack
            cfLoad.Quote = ""
            cfLoad.SkipLeadingRows = 1
            cfLoad.AllowJaggedRows = True
            cfLoad.SourceFormat = "CSV"
            jcf.Load = cfLoad
            jb.Configuration = jcf

            Dim jRef As New JobReference
            jRef.JobId = job_id.ToString
            jRef.ProjectId = project_id

            jb.JobReference = jRef

            'upload file
            Using fs As New FileStream(file_path, FileMode.Open)

                Dim media_upload As New JobsResource.InsertMediaUpload(BQ_Service, jb, project_id, fs, "application/octet-stream")

                Dim job_info = media_upload.UploadAsync

                While Not job_info.IsCompleted

                    'Console.WriteLine(job_info.Status)
                    Thread.Sleep(1000)
                End While

            End Using

            While True
                Dim poll_rsp = BQ_Service.Jobs.Get(project_id, job_id.ToString).Execute()
                If poll_rsp.Status.State.Equals("DONE") Then

                    'get job errors
                    If poll_rsp.Status.ErrorResult IsNot Nothing Then



                        'err_msg = poll_rsp.Status.ErrorResult.Message.ToString()
                        Dim err_rsn = poll_rsp.Status.ErrorResult.Reason
                        Dim err_loc = poll_rsp.Status.ErrorResult.Location


                        For Each eMsg In poll_rsp.Status.Errors
                            err_msg += eMsg.Message.ToString()
                            'Console.WriteLine(eMsg.Message.ToString())
                        Next

                        check_error(err_msg, retry_cnt)
                        If retry_cnt <= retry_limit Then
                            GoTo bq_start
                        Else
                            If File.Exists(file_path) Then
                                File.Delete(file_path)
                            End If
                            Exit Sub
                        End If

                    End If
                    Exit While
                End If
                Thread.Sleep(3000)
            End While

            'clean up
            File.Delete(file_path)

            If merge_tgt And does_dest_tbl_exist Then

                Dim join_clause As String
                Dim join_cols() As String = tgt_key_cols.Split(",")

                join_clause += "src." + Trim(join_cols(0)) + " = stg." + Trim(join_cols(0))

                For i As Integer = 1 To join_cols.Count - 1
                    join_clause += " AND src." + Trim(join_cols(i)) + " = stg." + Trim(join_cols(i))
                Next

                'get the target schema
                Dim tgt_cols As List(Of String) = get_bq_tbl_col_list(BQ_Service, project_nm, dataset_nm, dest_tbl_nm, err_msg)
                If err_msg <> "" Then Exit Sub



                'get the source schema
                Dim src_cols As List(Of String) = get_bq_tbl_col_list(BQ_Service, project_nm, dataset_nm, intermediate_tbl, err_msg)
                If err_msg <> "" Then Exit Sub


                Dim tgt_col_list As String
                Dim src_prefix_col_list As String

                Dim counter As Integer = 1
                For Each col In tgt_cols

                    If src_cols.Contains(col.ToString()) Then
                        If counter = 1 Then
                            tgt_col_list = col.ToString()
                            src_prefix_col_list = "src." + col.ToString()
                            counter += 1
                        Else
                            tgt_col_list += ", " + col.ToString()
                            src_prefix_col_list += ", src." + col.ToString()
                        End If
                    End If

                Next


                Dim merge_sql As String = "
                
                SELECT " + tgt_col_list + "
                FROM `" + project_nm + "." + dataset_nm + "." + intermediate_tbl + "`
                UNION ALL
                SELECT " + src_prefix_col_list + "
                FROM `" + project_nm + "." + dataset_nm + "." + dest_tbl_nm + "` as src
                    LEFT JOIN `" + project_nm + "." + dataset_nm + "." + intermediate_tbl + "` as stg
                        ON " + join_clause + "
                WHERE stg." + Trim(join_cols(0)) + " IS NULL                  
"

                log_edis_event("info", "BQ Export Merge SQL Created", merge_sql)


                bq_crt_tbl_from_qry_internal(BQ_Service, project_nm, dataset_nm, dest_tbl_nm, merge_sql, True, err_msg)

                If err_msg <> "" Then Exit Sub

                'drop the intermediate table
                drop_bq_tbl_internal(BQ_Service, project_nm, dataset_nm, intermediate_tbl, err_msg)
                If err_msg <> "" Then Exit Sub

                log_edis_event("info", "BQ Export Merge", "intermediate table dropped")

            End If



            'Dim done As DateTime = Now

            'Console.WriteLine("done at {0}", done.ToShortDateString + " " + done.ToShortTimeString)

            'Dim total_time_to_process_sec As Integer = DateDiff(DateInterval.Second, start, done)

            'Console.WriteLine("Total Processing Time (Seconds): {0}", total_time_to_process_sec)
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString()

            check_error(err_msg, retry_cnt)
            If retry_cnt <= retry_limit Then
                GoTo bq_start
            Else
                Try
                    If File.Exists(file_path) Then
                        File.Delete(file_path)
                    End If

                Catch ex1 As Exception

                End Try
                Exit Sub
            End If
        End Try

    End Sub

    Private Class bq_export_col
        Property col_nm As String
        Property data_type As String
        Property ord_pos As String

    End Class

    Shared Sub bq_crt_tbl_from_qry_internal(ByVal BQ_Service As BigqueryService, ByVal project_nm As String, ByVal dataset_nm As String, ByVal dest_tbl_nm As String,
                                            ByVal src_qry As String, ByVal replace As Boolean, ByRef err_msg As String)

        Try
            Dim job_id As Guid = Guid.NewGuid()
            bq_ref_job_id = job_id.ToString

            Dim jR As JobsResource = BQ_Service.Jobs()
            Dim thisJob As Job = New Job()

            Dim jRef As New JobReference
            jRef.JobId = job_id.ToString
            jRef.ProjectId = project_nm


            thisJob.JobReference = jRef

            Dim write_dispo As String = "WRITE_APPEND"
            If replace Then
                write_dispo = "WRITE_TRUNCATE"
            End If


            dataset_nm = get_cs_dataset_nm(BQ_Service, project_nm, dataset_nm, err_msg)
            If err_msg <> "" Then Exit Sub

            thisJob.Configuration = New JobConfiguration() With {.Query = New JobConfigurationQuery() With {
                .AllowLargeResults = True,
                .CreateDisposition = "CREATE_IF_NEEDED",
                .DefaultDataset = New DatasetReference() With {.ProjectId = project_nm, .DatasetId = dataset_nm},
                .DestinationTable = New TableReference() With {.ProjectId = project_nm, .DatasetId = dataset_nm, .TableId = dest_tbl_nm},
                .Query = src_qry,
                .WriteDisposition = write_dispo,
                .UseLegacySql = False
            }
            }

            Dim res = jR.Insert(thisJob, project_nm).Execute()

            'wait for finish
            While True
                Dim poll_rsp = BQ_Service.Jobs.Get(project_nm, job_id.ToString).Execute()
                If poll_rsp.Status.State.Equals("DONE") Then

                    'get job errors
                    If poll_rsp.Status.ErrorResult IsNot Nothing Then
                        'err_msg = poll_rsp.Status.ErrorResult.Message.ToString()
                        Dim err_rsn = poll_rsp.Status.ErrorResult.Reason
                        Dim err_loc = poll_rsp.Status.ErrorResult.Location


                        For Each eMsg In poll_rsp.Status.Errors
                            err_msg += eMsg.Message.ToString()
                            'Console.WriteLine(eMsg.Message.ToString())
                        Next

                    End If
                    Exit While
                End If
                Thread.Sleep(2000)
            End While

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try



    End Sub


    Shared Sub bq_crt_tbl_from_qry(ByRef err_msg As String)


        Dim retry_cnt As Integer = 0
        Dim cred As GoogleCredential
        Try


bq_start:

            Dim svc_uid As String = EDIS.get_edis_pval("svc_uid")

            'splite for project name/id
            Dim creds_str As String = EDIS.get_edis_pval("client_secrets")
            Dim project_nm As String = EDIS.get_edis_pval("project_nm")
            Dim project_id As String = EDIS.get_edis_pval("project_id")
            Dim dataset_nm As String = EDIS.get_edis_pval("dataset_nm")
            Dim dest_tbl_nm As String = EDIS.get_edis_pval("dest_tbl_nm")
            Dim src_qry As String = EDIS.get_edis_pval("src_qry")
            Dim drop_existing As Boolean = CBool(EDIS.get_edis_pval("drop_existing"))

            log_edis_event("info", "bq create table from select", "paramaters assigned (1)")

            Dim start As DateTime = Now

            Console.WriteLine("started at {0}", start.ToShortDateString + " " + start.ToShortTimeString)

            '--------------------------------------------------------------------------------------
            'Load Credentials
            cred = load_bq_svc_creds(creds_str, err_msg)
            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "Big Query", "credentials loaded")
            '--------------------------------------------------------------------------------------
            'Create Service

            Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)

            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "Big Query", "service created")


            Dim job_id As Guid = Guid.NewGuid()
            bq_ref_job_id = job_id.ToString

            ' The write dispo of WRITE_TRUNCATE is the same as a drop/recreate 
            ''if we are dropping the existing table
            'If drop_existing Then
            '    log_edis_event("info", "BigQuery", "dropping existing table check")
            '    Dim dropped As Boolean = drop_bq_tbl_internal(BQ_Service, project_id, dataset_nm, dest_tbl_nm, err_msg)
            '    If dropped Then
            '        log_edis_event("info", "BQ Create table (drop existing)", "done")
            '    End If
            '    If err_msg <> "" Then Exit Sub

            'End If

            If Not drop_existing AndAlso does_bq_tbl_exist(BQ_Service, project_id, dataset_nm, dest_tbl_nm, err_msg) Then
                If err_msg = "" Then
                    err_msg = "Destination table " + project_nm + "." + dataset_nm + "." + dest_tbl_nm + " already exists. If you want to replace it, please specify @drop_existing = 1 parameter when executing this task."
                End If
                Exit Sub
            End If

            If drop_existing Then
                bq_crt_tbl_from_qry_internal(BQ_Service, project_nm, dataset_nm, dest_tbl_nm, src_qry, True, err_msg)
            Else
                bq_crt_tbl_from_qry_internal(BQ_Service, project_nm, dataset_nm, dest_tbl_nm, src_qry, False, err_msg)
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Shared Sub bq_drop_table(ByRef err_msg As String)


        Dim retry_cnt As Integer = 0
        Dim cred As GoogleCredential
        Try


bq_start:

            Dim svc_uid As String = EDIS.get_edis_pval("svc_uid")

            'splite for project name/id
            Dim creds_str As String = EDIS.get_edis_pval("client_secrets")
            Dim project_nm As String = EDIS.get_edis_pval("project_nm")
            Dim project_id As String = EDIS.get_edis_pval("project_id")
            Dim dataset_nm As String = EDIS.get_edis_pval("dataset_nm")
            Dim dest_tbl_nm As String = EDIS.get_edis_pval("dest_tbl_nm")
            Dim fail_if_missing As String = EDIS.get_edis_pval("fail_if_missing")

            log_edis_event("info", "BQ Drop Table task", "paramaters assigned (1)")

            Dim start As DateTime = Now

            Console.WriteLine("started at {0}", start.ToShortDateString + " " + start.ToShortTimeString)

            '--------------------------------------------------------------------------------------
            'Load Credentials
            cred = load_bq_svc_creds(creds_str, err_msg)
            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BQ Drop Table task", "credentials loaded")
            '--------------------------------------------------------------------------------------
            'Create Service

            Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)

            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BQ Drop Table task", "service created")

            Dim res As Boolean = drop_bq_tbl_internal(BQ_Service, project_nm, dataset_nm, dest_tbl_nm, err_msg)

            If res = False AndAlso fail_if_missing = "1" Then
                err_msg = String.Format("Table {0} in dataset {1} does not exist", dest_tbl_nm, dataset_nm)
                Exit Sub
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Shared Sub import_data(ByRef err_msg As String)

        Dim retry_cnt As Integer = 0
        Dim cred As GoogleCredential
        Try

bq_start:

            'Console.WriteLine("started at {0}", Now.ToShortDateString + " " + Now.ToShortTimeString)
            log_edis_event("info", "BQ import task", "start")

            'Get Params
            Dim svc_uid As String = EDIS.get_edis_pval("svc_uid")

            'splite for project name/id
            Dim project_nm As String = EDIS.get_edis_pval("project_nm")
            Dim project_id As String = EDIS.get_edis_pval("project_id")

            Dim creds_str As String = EDIS.get_edis_pval("client_secrets")

            log_edis_event("info", "BQ Import", "svc account loaded")

            Dim BQ_fetch_size As Integer = EDIS.get_edis_pval("bq_fetch_size")
            Dim batch_size As Integer = EDIS.get_edis_pval("batch_size")
            Dim crt_dest_tbl As Boolean = CBool(EDIS.get_edis_pval("crt_dest_tbl"))

            'log_edis_event("info", "select BQ import task", "paramaters assigned (1)")

            Dim mssql_inst As String = EDIS._mssql_inst
            Dim tgt_db As String = EDIS.get_edis_pval("tgt_db")
            Dim tgt_schema As String = EDIS.get_edis_pval("tgt_schema")
            Dim tgt_tbl As String = EDIS.get_edis_pval("tgt_tbl")
            Dim max_col_width As Integer = EDIS.get_edis_pval("max_col_width")

            log_edis_event("info", "max_col_width", max_col_width.ToString)


            ' log_edis_event("info", "select BQ import task", "paramaters assigned (2)")

            Dim load_on_ordinal As Boolean = CBool(EDIS.get_edis_pval("load_on_ordinal"))

            Dim src_qry As String = EDIS.get_edis_pval("src_qry")

            Dim dest_cn_str As String = EDIS.get_edis_pval("dest_cn_str")
            Dim dest_sql_instance As String = cDT_Tasks.get_cn_str_prop(dest_cn_str, "DATA_SOURCE", err_msg)
            If err_msg <> "" Then Exit Sub

            'QA
            'log_edis_event("info", "dest sql instance", dest_sql_instance)
            'log_edis_event("info", "dest cn_str", dest_cn_str)

            Dim dest_sql_user_id As String = cDT_Tasks.get_cn_str_prop(dest_cn_str, "USER_ID", err_msg)
            If err_msg <> "" Then Exit Sub
            Dim dest_sql_password As String = cDT_Tasks.get_cn_str_prop(dest_cn_str, "PASSWORD", err_msg)
            If err_msg <> "" Then Exit Sub

            'QA
            ' log_edis_event("info", "dest sql user id", dest_sql_user_id)
            'log_edis_event("info", "dest sql password", dest_sql_password)

            Dim tgt_col_list As New List(Of String)

            '--------------------------------------------------------------------------------------
            'Load Credentials
            cred = load_bq_svc_creds(creds_str, err_msg)
            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BQ import task", "credentials loaded")
            '--------------------------------------------------------------------------------------
            'Create Service

            Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)

            If err_msg <> "" Then
                check_error(err_msg, retry_cnt)
                If retry_cnt <= retry_limit Then
                    GoTo bq_start
                Else
                    Exit Sub
                End If
            End If

            log_edis_event("info", "BQ import task", "service created")

            '--------------------------------------------------------------------------------------
            'Create Job

            Dim jb As JobsResource = BQ_Service.Jobs

            Dim query_req As QueryRequest = New QueryRequest()
            query_req.Query = src_qry
            query_req.MaxResults = BQ_fetch_size
            query_req.UseLegacySql = False

            '6 hours
            'query_req.TimeoutMs = 6000000 
            '                     21600000
            query_req.TimeoutMs = bq_qry_timeout_ms


            '--------------------------------------------------------------------------------------
            'Run the query

            Dim start_time As DateTime = Now

            Dim query_wait_start_time As DateTime = Now

            Dim job_processing_sw As New Stopwatch
            Dim page_fetch_sw As New Stopwatch

            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_JOB_START_TS>{0}</BQ_IMPORT_JOB_START_TS>", EDIS.get_now_as_string))

            job_processing_sw.Start()

            Dim queryResp As QueryResponse = jb.Query(query_req, project_id).Execute()
            bq_ref_job_id = queryResp.JobReference.JobId.ToString

            'Dim curr_time As DateTime = Now

            'log_edis_event("info", "bigquery import performance", String.Format("Query Response processing time {0}", DateDiff(DateInterval.Second, start_time, curr_time)))



            'wait for it to finish
            While True
                Dim poll_rsp = BQ_Service.Jobs.Get(queryResp.JobReference.ProjectId, queryResp.JobReference.JobId).Execute()
                If poll_rsp.Status.State.Equals("DONE") Then

                    'refresh the query response
                    'queryResp = jb.Query(query_req, project_id).Execute()

                    'jb.GetQueryResults(project_id, queryResp.JobReference.JobId).Execute()

                    'get job errors
                    If poll_rsp.Status.ErrorResult IsNot Nothing Then
                        'err_msg = poll_rsp.Status.ErrorResult.Message.ToString()
                        Dim err_rsn = poll_rsp.Status.ErrorResult.Reason
                        Dim err_loc = poll_rsp.Status.ErrorResult.Location


                        For Each eMsg In poll_rsp.Status.Errors
                            err_msg += eMsg.Message.ToString()
                            'Console.WriteLine(eMsg.Message.ToString())
                        Next

                    End If
                    Exit While
                End If
                Thread.Sleep(1000)
            End While

            job_processing_sw.Stop()

            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_JOB_END_TS>{0}</BQ_IMPORT_JOB_END_TS>", EDIS.get_now_as_string))

            'Dim query_wait_end_time As DateTime = Now
            'Dim total_query_wait_time As Integer = DateDiff(DateInterval.Second, query_wait_start_time, query_wait_end_time)

            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_JOB_TOTAL_TIME_MS>{0}</BQ_IMPORT_JOB_TOTAL_TIME_MS>", job_processing_sw.ElapsedMilliseconds))

            log_edis_event("info", "BigQuery Import", String.Format("query wait time MS {0}", job_processing_sw.ElapsedMilliseconds))

            Dim schema_ref_cnt As Integer = 0
            While True
                If queryResp.Schema Is Nothing Then
                    schema_ref_cnt += 1
                    log_edis_event("info", "BQ import task", "waiting on scheme refresh for results")
                    Thread.Sleep(2000)
                Else
                    Exit While
                End If

                If schema_ref_cnt >= 15 Then
                    err_msg = "Unable to refresh schema for BQ data import"
                    Exit Sub
                End If
            End While

            log_edis_event("info", "BigQuery Import", "query schema retrieved")

            '---------------------------------------------------------------------------------------
            'Get the source Query Schema
            Dim src_cols As New List(Of cDataTypeParser.src_column_metadata)
            Dim i As Integer = 0
            For i = 0 To queryResp.Schema.Fields.Count - 1
                Dim col = queryResp.Schema.Fields(i)
                Dim src_col As New cDataTypeParser.src_column_metadata
                src_col.col_nm = col.Name
                src_col.ord_pos = i
                Select Case col.Type.ToString.ToLower
                   ' Case "integer"
                     '   src_col.data_type = "int"
                    Case "int64", "integer"
                        src_col.data_type = "bigint"
                    Case "float", "float64"
                        src_col.data_type = "float"
                    Case "date"
                        src_col.data_type = "date"
                    Case "time"
                        src_col.data_type = "time"
                    Case "timestamp"
                        src_col.data_type = "timestamp"
                    Case "datetime"
                        src_col.data_type = "datetime2"
                    Case "bool", "boolean"
                        src_col.data_type = "bit"
                    Case Else
                        src_col.data_type = "nvarchar"
                        src_col.max_len = max_col_width
                End Select
                src_cols.Add(src_col)

                log_edis_event("info", "BigQuery Import Task", String.Format("Metadata from source for Column {0}: Data type: {1}", src_col.col_nm, src_col.data_type))

            Next



            'create datatable shell
            Dim dt As New DataTable



            For Each col In src_cols
                Select Case col.data_type.ToLower
                    'Case "int"
                     '   dt.Columns.Add(col.col_nm, GetType(Int32))
                    Case "bigint", "int"
                        dt.Columns.Add(col.col_nm, GetType(Int64))
                    Case "float"
                        dt.Columns.Add(col.col_nm, GetType(Double))
                    Case "datetime2", "date", "time", "timestamp"
                        dt.Columns.Add(col.col_nm, GetType(Date))
                    Case "bit"
                        dt.Columns.Add(col.col_nm, GetType(Boolean))
                    Case Else
                        dt.Columns.Add(col.col_nm, GetType(String))
                End Select
            Next

            log_edis_event("info", "BigQuery Import", "data table created")

            dt.MinimumCapacity = batch_size

            'create sql table
            If crt_dest_tbl Then
                log_edis_event("info", "BQ import task", "creating destination table")
                cDT_Tasks.create_dest_tbl(dest_sql_instance, tgt_db, tgt_schema, tgt_tbl, src_cols, False, err_msg)
                If err_msg <> "" Then Exit Sub
                log_edis_event("info", "BigQuery Import Task", "destination table created")
            End If



            tgt_col_list = cDT_Tasks.get_tgt_tbl_cols(dest_sql_instance, tgt_db, tgt_schema, tgt_tbl, err_msg)
            If err_msg <> "" Then Exit Sub

            '---------------------------------------------------------------------------------------------------------
            'Page Through the results

            Dim _row_index As Integer = 0
            Dim curr_batch_row_cnt As Integer = 0
            Dim page_token As String = Nothing

            Dim batch_id As Integer = 0

            log_edis_event("info", "BQ import task", "fetching rows start")

            Dim page_cnt As Integer = 0

            Dim total_page_fetch_time As Integer = 0

            Dim data_processing_sw As New Stopwatch

            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_RESULTS_FETCH_START_TS>{0}</BQ_IMPORT_RESULTS_FETCH_START_TS>", EDIS.get_now_as_string))

            While True

                page_fetch_sw.Start()
                Dim page_fetch_start As DateTime = Now
                log_edis_event("info", "BigQuery Import Task", String.Format("Page {0} Fetch START", page_cnt))

                Dim resPage = BQ_Service.Jobs.GetQueryResults(queryResp.JobReference.ProjectId, queryResp.JobReference.JobId)

                'if there werwe no results at all, skip this process
                If resPage Is Nothing Then
                    page_fetch_sw.Stop()
                    Exit While
                End If



                'set the page token before making the call/execute
                resPage.PageToken = page_token


                Dim res = resPage.Execute()

                page_fetch_sw.Stop()
                Dim page_fetch_finish As DateTime = Now
                Dim curr_page_fetch_time As Integer = DateDiff(DateInterval.Second, page_fetch_start, page_fetch_finish)
                total_page_fetch_time += curr_page_fetch_time
                log_edis_event("info", "BigQuery Import Task", String.Format("Page {0} Fetch DONE", page_cnt))

                log_edis_event("info", "BigQuery Import Task", String.Format("Page {0} Fetch time {1}", page_cnt, curr_page_fetch_time))

                'grab the page token if its still there
                page_token = res.PageToken

                If page_token IsNot Nothing Then
                    log_edis_event("info", "BigQuery Import Task", String.Format("Page Token {0}", page_token))
                End If



                Dim page_row_process_start As DateTime

                log_edis_event("info", "BigQuery Import Task", String.Format("Processing rows from Page {0}", page_cnt))

                If res.Rows Is Nothing Then
                    log_edis_event("info", "BigQuery Import Task", "now rows to retrieve, exiting proc")
                    Exit While
                End If


                data_processing_sw.Start()

                For Each row In res.Rows
                    Dim dr As DataRow = dt.NewRow

                    For i = 0 To src_cols.Count - 1

                        Dim f = row.F(i).V
                        'If f Is Nothing Then
                        '    dr(i) = DBNull.Value
                        If String.IsNullOrEmpty(f) Then
                            dr(i) = DBNull.Value
                        Else
                            Select Case src_cols(i).data_type.ToLower
                                'timestamps are in unix....need to convert
                                Case "timestamp" ' "datetime2"
                                    Dim ts As DateTime = New DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc)
                                    ts = ts.AddSeconds(f.ToString).ToUniversalTime()
                                    dr(i) = ts
                                Case Else
                                    dr(i) = f
                            End Select

                        End If

                    Next

                    'add the row
                    dt.Rows.Add(dr)
                    _row_index += 1
                    curr_batch_row_cnt += 1

                    If curr_batch_row_cnt >= batch_size Then

                        log_edis_event("info", "BigQuery Import Task", "Starting BULK INSERT")

                        'batch_synchronizer.Add(batch_id, False)
                        cDT_Tasks.bulk_insert(dest_sql_instance, tgt_db, tgt_schema, tgt_tbl, tgt_col_list, dt, batch_size, load_on_ordinal, dest_sql_user_id, dest_sql_password, err_msg)

                        log_edis_event("info", "BigQuery Import Task", "BULK INSERT Complete")

                        If err_msg <> "" Then Exit Sub
                        dt.Clear()
                        'Console.WriteLine("Batch ID {0} submitted to MSSQL; Total row count is {1}", batch_id, _row_index)
                        curr_batch_row_cnt = 0
                        batch_id += 1
                    End If

                Next

                log_edis_event("info", "Bigquery Import Task", String.Format("Rows from Page {0} processed", page_cnt))

                'Are we at the end??
                If res.PageToken Is Nothing Then
                    If curr_batch_row_cnt >= 1 Then

                        log_edis_event("info", "BigQuery Import Task", "Starting BULK INSERT")

                        cDT_Tasks.bulk_insert(dest_sql_instance, tgt_db, tgt_schema, tgt_tbl, tgt_col_list, dt, batch_size, load_on_ordinal, dest_sql_user_id, dest_sql_password, err_msg)
                        If err_msg <> "" Then Exit Sub

                        log_edis_event("info", "BigQuery Import Task", "BULK INSERT Complete")

                        'Console.WriteLine("Batch ID {0} submitted to MSSQL; Total row count is {1}", batch_id, _row_index)
                        dt.Clear()
                    End If

                    data_processing_sw.Stop()
                    Exit While
                End If

                data_processing_sw.Stop()

                log_edis_event("info", "BigQuery Import Task", String.Format("Page {0} processed and loaded", page_cnt))

                page_cnt += 1


            End While



            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_RESULTS_FETCH_END_TS>{0}</BQ_IMPORT_RESULTS_FETCH_END_TS>", EDIS.get_now_as_string))
            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_RESULTS_TOTAL_PAGES_FETCHED>{0}</BQ_IMPORT_RESULTS_TOTAL_PAGES_FETCHED>", page_cnt))
            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_RESULTS_TOTAL_PAGE_FETCH_TIME_MS>{0}</BQ_IMPORT_RESULTS_TOTAL_PAGE_FETCH_TIME_MS>", page_fetch_sw.ElapsedMilliseconds))
            EDIS.info_msgs.Append(String.Format("<BQ_IMPORT_RESULTS_DATA_PROCESSING_TIME_MS>{0}</BQ_IMPORT_RESULTS_DATA_PROCESSING_TIME_MS>", data_processing_sw.ElapsedMilliseconds))

skip_row_processing:


            'free objects
            queryResp = Nothing
            BQ_Service = Nothing
            cred = Nothing
            GC.Collect()

            EDIS.log_rows_tsfr(_row_index)

            'Console.WriteLine("finished at {0}", Now.ToShortDateString + " " + Now.ToShortTimeString)

            Dim total_time_to_process_sec As Integer = DateDiff(DateInterval.Second, start_time, Now)

            log_edis_event("info", "BigQuery Import", String.Format("Total Processing Time {0}", total_time_to_process_sec))

            'Console.WriteLine("Total Processing Time (Seconds): {0}", total_time_to_process_sec)

        Catch ex As Exception
            err_msg = "BQ Import Data Error: " + ex.Message.ToString + vbCrLf + ex.GetBaseException.Message.ToString() '+ ex.StackTrace.ToString
            check_error(err_msg, retry_cnt)
            If retry_cnt <= retry_limit Then
                GoTo bq_start
            Else
                Exit Sub
            End If

        End Try
    End Sub

    Shared Sub check_error(ByRef err_msg As String, ByRef retry_cnt As String)
        If err_msg <> "" Then
            If InStr(err_msg, "No Data retrieved") > 0 Then
                If retry_cnt > retry_limit Then
                    Exit Sub
                    'Throw New Exception(err_msg)
                Else

                    retry_cnt += 1
                    log_edis_event("info", "BigQuery Import", String.Format("retry count updated {0}", retry_cnt))
                    Console.WriteLine("retry count updated {0}...sleeping on it", retry_cnt)
                    err_msg = ""

                    Thread.Sleep(5000)

                End If
            Else
                retry_cnt += 1
                Console.WriteLine("retry count updated {0}...sleeping on it", retry_cnt)
                Exit Sub
                'Throw New Exception(err_msg)
            End If
        End If
    End Sub



    Private Shared Function load_bq_svc_creds(creds_str As String, ByRef err_msg As String) As GoogleCredential

        Try
            Dim stream As New MemoryStream
            Dim sw As New StreamWriter(stream)
            sw.Write(creds_str)
            sw.Flush()
            stream.Position = 0

            Dim cred As GoogleCredential
            cred = GoogleCredential.FromStream(stream)

            Dim scopes As String() = New String() {BigqueryService.Scope.Bigquery, BigqueryService.Scope.CloudPlatform}
            cred = cred.CreateScoped(scopes)

            Return cred
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Function

    Private Shared Function create_bq_service(ByVal cred As GoogleCredential, ByVal project_nm As String, ByRef err_msg As String) As BigqueryService
        Try
            Dim Service As New BigqueryService(
            New BaseClientService.Initializer() With {.HttpClientInitializer = cred, .ApplicationName = project_nm})
            Return Service
        Catch ex As Exception
            err_msg = "Service Create Error: " + ex.Message.ToString()
        End Try

    End Function



End Class
