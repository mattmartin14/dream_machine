Class cBigQueryTest
    'clears the login cache for GCP
    'Shared Sub clear_login(ByRef err_msg As String)
    '    Try
    '        log_edis_event("info", "clear login", "starting process")
    '        Dim client_id As String = EDIS.get_edis_pval("svc_uid")
    '        Dim client_pwd As String = EDIS.get_edis_pval("svc_pwd")
    '        Dim project_nm As String = EDIS.get_edis_pval("project_nm")
    '        Dim cred As UserCredential = load_bq_creds(client_id, client_pwd, err_msg)
    '        If err_msg <> "" Then
    '            Exit Sub
    '        End If
    '        log_edis_event("info", "clear login", "credentials loaded")
    '        Dim BQ_Service As BigqueryService = create_bq_service(cred, project_nm, err_msg)
    '        If err_msg <> "" Then
    '            Exit Sub
    '        End If
    '        log_edis_event("info", "clear login", "service created, revoking token")
    '        cred.RevokeTokenAsync(CancellationToken.None).Wait()
    '        log_edis_event("info", "clear login", "process complete")
    '    Catch ex As Exception
    '        err_msg = ex.Message.ToString
    '    End Try
    'End Sub


    'Private Shared Sub revoke_token(ByVal creds As UserCredential)
    '    creds.RevokeTokenAsync(CancellationToken.None).Wait()
    'End Sub

    'Private Shared Function load_bq_creds(ByVal client_id As String, ByVal client_pwd As String, ByRef err_msg As String) As UserCredential

    '    Try

    '        Dim creds_str As String = "
    '        {
    '          ""web"": {
    '            ""client_id"": """ + client_id + """,
    '            ""client_secret"":""" + client_pwd + """,
    '            ""redirect_uris"": [""http//localhost"", ""urn:ietf : wg : oauth : 2.0:oob""],
    '            ""auth_uri"": ""https: //accounts.google.com/o/oauth2/auth"",
    '            ""token_uri"": ""https://accounts.google.com/o/oauth2/token""
    '          }
    '        }
    '        "
    '        Dim stream As New MemoryStream
    '        Dim sw As New StreamWriter(stream)
    '        sw.Write(creds_str)
    '        sw.Flush()
    '        stream.Position = 0

    '        Dim cred As UserCredential

    '        cred = GoogleWebAuthorizationBroker.AuthorizeAsync(GoogleClientSecrets.Load(stream).Secrets, {BigqueryService.Scope.Bigquery}, "user", CancellationToken.None).Result

    '        Return cred
    '    Catch ex As Exception
    '        err_msg = "Credentials Error: " + ex.Message.ToString()
    '    End Try

    'End Function

    'Private Class src_metadata
    '    Property col_nm As String
    '    Property data_type As String
    '    Property ord_pos As String
    'End Class

    'Private Shared Sub crt_dest_tbl(ByVal cols As List(Of src_metadata), ByVal mssql_inst As String, ByVal tbl_nm As String, ByVal db_nm As String, ByRef err_msg As String)

    '    Try
    '        'Loop through columns to creat alter statement
    '        Dim sql_alter_tbl As String = "ALTER TABLE [" + tbl_nm + "] ADD "

    '        'add first col
    '        sql_alter_tbl += "[" + cols(0).col_nm + "] " + get_sql_datatype(cols(0).data_type)

    '        For i = 1 To cols.Count - 1
    '            sql_alter_tbl += ", [" + cols(i).col_nm + "] " + get_sql_datatype(cols(i).data_type)
    '        Next

    '        Using cn As New SqlConnection("server = " + mssql_inst + "; Integrated Security = True; Initial Catalog = " + db_nm + "")
    '            cn.Open()

    '            Using cmd As New SqlCommand(sql_alter_tbl, cn)
    '                cmd.ExecuteNonQuery()
    '            End Using

    '            'drop the first column
    '            Dim first_col_sql As String = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME = @tbl_nm"
    '            Dim first_col As String
    '            Using cmd As New SqlCommand(first_col_sql, cn)
    '                cmd.Parameters.AddWithValue("@tbl_nm", tbl_nm)
    '                first_col = cmd.ExecuteScalar().ToString
    '            End Using

    '            Dim drop_first_col_sql As String = "ALTER TABLE [" + tbl_nm + "] DROP COLUMN [" + first_col + "]"
    '            Using cmd As New SqlCommand(drop_first_col_sql, cn)
    '                cmd.ExecuteNonQuery()
    '            End Using

    '            cn.Close()
    '        End Using
    '    Catch ex As Exception
    '        err_msg = "Create Destination Table Error: " + ex.Message.ToString
    '    End Try

    'End Sub

    'Private Shared Function get_sql_datatype(src_data_type As String) As String

    '    Select Case src_data_type.ToLower
    '        Case "integer"
    '            Return "int"
    '        Case "int64"
    '            Return "bigint"
    '        Case "float", "float64"
    '            Return "float"
    '        Case "date"
    '            Return "date"
    '        Case "datetime", "timestamp"
    '            Return "datetime2"
    '        Case Else
    '            Return "nvarchar(4000)"
    '    End Select

    'End Function

    'Private Shared Async Sub bulk_copy_async(ByVal mssql_inst As String, ByVal target_tbl_nm As String, ByVal dt As DataTable, ByVal db_nm As String, ByVal batch_id As Integer)

    '    Try
    '        Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; Integrated Security = True; Initial Catalog = " + db_nm + "")
    '            Await cn.OpenAsync().ConfigureAwait(False)

    '            Dim dt_to_load As New DataTable
    '            dt_to_load = dt.Copy


    '            Using bc As New SqlBulkCopy(cn, SqlBulkCopyOptions.TableLock, Nothing)
    '                bc.BulkCopyTimeout = 0
    '                bc.DestinationTableName = target_tbl_nm
    '                Await bc.WriteToServerAsync(dt_to_load)
    '                Console.WriteLine("Batch ID {0} Bulk insert complete", batch_id)
    '                'update the batch synchronizer
    '                batch_synchronizer.Item(batch_id) = True

    '                dt_to_load.Clear()
    '                dt_to_load.Dispose()
    '            End Using

    '        End Using
    '    Catch ex As Exception
    '        'how does one notify an error on an async task??
    '        'err_msg = "Bulk Copy Error: " + ex.Message.ToString()
    '        'Throw New Exception("Bulk Copy Error: " + ex.Message.ToString())
    '    End Try

    'End Sub


    'Private Shared Sub bulk_copy(ByVal mssql_inst As String, ByVal target_tbl_nm As String, ByVal dt As DataTable, ByVal db_nm As String, ByRef err_msg As String)

    '    Try
    '        Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; Integrated Security = True; Initial Catalog = " + db_nm + "")
    '            cn.Open()

    '            Using bc As New SqlBulkCopy(cn, SqlBulkCopyOptions.TableLock, Nothing)
    '                bc.BulkCopyTimeout = 0
    '                bc.DestinationTableName = target_tbl_nm
    '                bc.WriteToServer(dt)
    '            End Using

    '        End Using
    '    Catch ex As Exception
    '        err_msg = "Bulk Copy Error: " + ex.Message.ToString()
    '        'Throw New Exception("Bulk Copy Error: " + ex.Message.ToString())
    '    End Try

    'End Sub

End Class
