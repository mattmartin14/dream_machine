
Class cCreateDestTbl
    Inherits data_tsfr

    Shared Sub create_dest_tbl(ByVal src_meta_coll As Collection, ByRef err_msg As String)

        Dim dest_cn_str As String

        Try
            Dim dest_sys_nm As String = EDIS.get_edis_pval("dest_sys_nm")
            Dim dest_tbl As String = EDIS.get_edis_pval("dest_tbl")
            Dim crt_dest_tbl As Boolean = CBool(EDIS.get_edis_pval("crt_dest_tbl"))
            Dim col_place_holder As String = EDIS.get_edis_pval("col_place_holder")
            dest_cn_str = EDIS.get_edis_pval("dest_cn_str")
            'Dim dest_cn_type As String = EDIS.get_edis_pval("dest_cn_type")
            Dim dest_server_provider As String = EDIS.get_edis_pval("dest_server_provider")
            Dim dest_server_sub_provider As String = EDIS.get_edis_pval("dest_server_sub_provider")
            Dim dest_platform As String = EDIS.get_edis_pval("dest_platform")
            Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")
            Dim file_path As String = EDIS.get_edis_pval("file_path")
            'Dim tgt_db As String = EDIS.get_edis_pval("tgt_db")
            'Dim tgt_schema As String = EDIS.get_edis_pval("tgt_schema")

            If dest_server_provider.ToUpper = "EXCEL" Then ' UCase(dest_cn_type) = "EXCEL" Then
                dest_cn_str = mCn_builder.build_cn_str("EXCEL", file_path, False, True)
            End If

            'we need to figure out if this is a table on the host server for EDIS...if so, its already been materialized, and we need to add columns
            Dim host_mssql_inst As String = EDIS._mssql_inst
            Dim is_dest_tbl_on_host As Boolean = False
            If LCase(host_mssql_inst) = LCase(dest_sys_nm) Then
                is_dest_tbl_on_host = True
            End If

            Dim col_pos As Integer = 1

            'if the destination table is a global temp on the host sql server, we run an alter statement, otherwise, a create statement
            Dim tbl_sql_cmd As String = ""

            If is_dest_tbl_on_host = True Then
                tbl_sql_cmd = "ALTER TABLE " & dest_tbl & " ADD "
            Else
                tbl_sql_cmd = "CREATE TABLE " & dest_tbl & " ( "
            End If

            'loop through source metadata and build the create statement
            For Each col As src_meta In src_meta_coll

                'get the translated data type
                Dim xref_data_type As String = data_type_translator(col.data_type_str, dest_platform, col.col_length, col.col_prec, col.col_scale)

                'add to alter statement
                'if we are after the first column getting added, place a comma in
                If col_pos > 1 Then tbl_sql_cmd = tbl_sql_cmd + ", "

                'edit 12/9/17 to handle ssas columns that start with the square brackets
                Dim curr_col_nm As String
                If Left(col.col_nm, 1) = "[" And Right(col.col_nm, 1) = "]" Then
                    curr_col_nm = """" + col.col_nm + """"
                Else
                    curr_col_nm = "[" + col.col_nm + "]"
                End If


                tbl_sql_cmd = tbl_sql_cmd + " " + curr_col_nm + " " + xref_data_type + " "

                'add the column
                'tbl_sql_cmd = tbl_sql_cmd + " [" + col.col_nm + "] " + xref_data_type + " "

                col_pos += 1

            Next

            If is_dest_tbl_on_host = True Then

                Dim dest_tbl_db As String = EDIS.get_edis_pval("dest_tbl_db")

                Using cn1 As New SqlClient.SqlConnection("Server=" + mssql_inst + "; Integrated Security = SSPI; Initial Catalog = " + dest_tbl_db)
                    cn1.Open()

                    '-- update to go via object id of database
                    Dim full_object_path As String
                    If dest_tbl_db = "tempdb" Then
                        full_object_path = "tempdb.." + dest_tbl
                    Else
                        full_object_path = dest_tbl
                    End If

                    Dim first_col_sql As String = "SELECT name from sys.columns where object_id = object_id('" + full_object_path + "') and column_id = 1"

                    'Dim first_col_sql As String = "select COLUMN_NAME from INFORMATION_SCHEMA.columns where TABLE_NAME = '" + dest_tbl + "'"

                    Dim first_col As String = ""

                    Using cmd1 As New SqlClient.SqlCommand(first_col_sql, cn1)
                        first_col = cmd1.ExecuteScalar().ToString()
                    End Using

                    tbl_sql_cmd = tbl_sql_cmd & "; ALTER TABLE " & dest_tbl & " DROP COLUMN [" & first_col & "]" '
                    cn1.Close()
                End Using

            Else
                tbl_sql_cmd += ")"
            End If

            'if its ado.net get the sub provider
            Dim provider_to_exec As String
            If dest_server_provider.ToUpper = "ADO.NET" Then
                provider_to_exec = dest_server_sub_provider.ToUpper
            Else
                provider_to_exec = dest_server_provider.ToUpper
            End If

            Select Case LCase(provider_to_exec)
                Case "mssql"
                    dft_functions.run_mssql_cmd(tbl_sql_cmd, dest_cn_str, err_msg)
                Case "oledb", "excel"
                    dft_functions.run_oledb_cmd(tbl_sql_cmd, dest_cn_str, err_msg)
                Case "odbc"
                    dft_functions.run_odbc_cmd(tbl_sql_cmd, dest_cn_str, err_msg)
                Case Else
                    err_msg = "Provider for creating the destination table not recongized"
                    Exit Sub
            End Select

        Catch ex As Exception
            err_msg = "Error generating destination table:"
            err_msg += vbCrLf

            Dim sys_err_msg As String = ex.Message.ToString()

            err_msg += sys_err_msg

            'if the error message was login failure..then reformat it to a better error message
            If sys_err_msg.ToLower Like "*cannot open database * requested by the login. the login failed.*" Then
                Dim err_msg2 As String
                Dim fmt_msg As String = get_login_err_fmt_msg(sys_err_msg, dest_cn_str, err_msg2)

                'if there were no errors parsing...set the error message to the formatted login message
                If String.IsNullOrWhiteSpace(err_msg2) Then
                    err_msg = fmt_msg
                Else
                    err_msg += vbCrLf + "iError: Formatting Login Failure for Dest Table: " + vbCrLf + err_msg2
                End If

            End If

        End Try

    End Sub

    Private Shared Function get_login_err_fmt_msg(sys_err_msg As String, dest_cn_str As String, ByRef err_msg As String) As String

        Try
            Dim usr_id As String
            Dim tgt_db As String
            Dim tgt_srvr As String



            'parse the user id
            Dim login_usr_start_pos As Integer = sys_err_msg.ToLower.IndexOf("login failed for user ")
            usr_id = Mid(sys_err_msg, login_usr_start_pos, Len(sys_err_msg) - login_usr_start_pos)
            usr_id = usr_id.Replace("Login failed for user '", "")
            usr_id = usr_id.Replace("'", "")
            usr_id = usr_id.Replace(vbLf, "")

            'parse the target db
            Dim tgt_db_start_pos As Integer = sys_err_msg.ToLower.IndexOf("cannot open database")
            Dim tgt_db_end_pos As Integer = sys_err_msg.ToLower.IndexOf(""" requested by the login")

            tgt_db = Mid(sys_err_msg, tgt_db_start_pos, tgt_db_end_pos - tgt_db_start_pos + 1)
            tgt_db = tgt_db.Replace("Cannot open database """, "")
            tgt_db = tgt_db.Replace(vbLf, "")

            'Console.WriteLine(usr_id)
            'Console.WriteLine(tgt_db)

            'parse the server out of the dest cn string
            Dim cn_parts As String() = dest_cn_str.Split(";")
            For Each part In cn_parts
                If Trim(part.Split("=")(0).ToLower) = "data source" Or Trim(part.Split("=")(0).ToLower) = "server" Then
                    tgt_srvr = Trim(part.Split("=")(1).ToString)
                    Exit For
                End If
            Next

            ' Console.WriteLine(tgt_srvr)

            Dim final_msg As String = ""
            final_msg += "Create Destination Table Failed"
            final_msg += vbCrLf
            final_msg += "User [" + usr_id + "] is not a valid login for database [" + tgt_db + "] in SQL Server [" + tgt_srvr + "]."
            final_msg += vbCrLf
            final_msg += "Add the user to the SQL Server, mapped to the database prior to re-running this statement."
            final_msg += vbCrLf
            final_msg += "Please note that the user will need CREATE permissions on the target database to create destination tables."


            Return final_msg

        Catch ex As Exception
            err_msg = "create dest table msg parser error: " + ex.Message.ToString()
            Return Nothing
        End Try



    End Function

End Class
