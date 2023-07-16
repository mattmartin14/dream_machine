
Imports System.Data.SqlClient


Friend Class cDT_Tasks

    'Friend Shared Sub bulk_insert_from_stream(ByVal mssql_inst As String, ByVal tgt_db As String, tgt_schema As String, tgt_tbl As String,
    '                              tgt_cols As List(Of String), ByVal reader As IDataReader,
    '                              ByVal batch_size As String, ByVal load_on_ordinal As Boolean, ByRef err_msg As String)

    '    Dim err_msg_prefix As String = "Bulk Insert Error (S): "
    '    Try
    '        Dim cn_str As String = String.Format("Server = {0}; Integrated Security = True; Database = {1}", mssql_inst, tgt_db)

    '        Using cn As New SqlClient.SqlConnection(cn_str)
    '            cn.Open()

    '            Using bc As New SqlBulkCopy(cn_str, SqlBulkCopyOptions.TableLock)
    '                bc.BatchSize = batch_size
    '                bc.BulkCopyTimeout = 0
    '                bc.EnableStreaming = True
    '                bc.DestinationTableName = "[" + tgt_schema + "].[" + tgt_tbl + "]"
    '                'Bulk copy default is ordinal
    '                If Not load_on_ordinal Then
    '                    For i As Integer = 0 To reader.FieldCount - 1
    '                        Dim mp As New SqlClient.SqlBulkCopyColumnMapping()
    '                        mp.SourceColumn = reader.GetName(i)
    '                        mp.DestinationColumn = get_dest_col(reader.GetName(i), tgt_cols)
    '                        bc.ColumnMappings.Add(mp)
    '                    Next
    '                End If

    '                bc.WriteToServer(reader)

    '            End Using

    '        End Using
    '    Catch ex As Exception
    '        err_msg = err_msg_prefix + ex.Message.ToString()
    '    End Try

    'End Sub


    Friend Shared Sub bulk_insert(ByVal mssql_inst As String, ByVal tgt_db As String, tgt_schema As String, tgt_tbl As String,
                                  tgt_cols As List(Of String), ByVal dt As DataTable,
                                  ByVal batch_size As String, ByVal load_on_ordinal As Boolean,
                                  sql_user_id As String, sql_password As String, ByRef err_msg As String)

        Dim err_msg_prefix As String = "Bulk Insert Error: "
        Try
            Dim cn_str As String

            'if not supplying a user id, then it uses integrated security
            If String.IsNullOrWhiteSpace(sql_user_id) Then
                cn_str = String.Format("Server = {0}; Integrated Security = True; Database = {1}", mssql_inst, tgt_db)
                log_edis_event("info", "Bulk Insert", "connection set using integrated security")
            Else
                cn_str = String.Format("Server = {0}; User ID = {1}; Password = {2}; Database = {3}", mssql_inst, sql_user_id, sql_password, tgt_db)
                log_edis_event("info", "Bulk Insert", "connection set using SQL Server Authentication")
            End If

            Using cn As New SqlClient.SqlConnection(cn_str)
                cn.Open()

                Using bc As New SqlBulkCopy(cn_str, SqlBulkCopyOptions.TableLock)
                    bc.BatchSize = batch_size
                    bc.BulkCopyTimeout = 0
                    bc.DestinationTableName = "[" + tgt_schema + "].[" + tgt_tbl + "]"
                    'Bulk copy default is ordinal
                    If Not load_on_ordinal Then
                        For Each col In dt.Columns
                            Dim mp As New SqlClient.SqlBulkCopyColumnMapping()
                            mp.SourceColumn = col.ToString
                            log_edis_event("info", "Bulk Insert Mapping", "mapping column " + col.ToString)
                            Dim dest_col As String = get_dest_col(col.ToString, tgt_cols)
                            If String.IsNullOrWhiteSpace(dest_col) Then
                                err_msg = "Error linking column [" + col.ToString + "] to destination table [" + tgt_db + "].[" + tgt_schema + "].[" + tgt_tbl + "]. Column name does not exist on the destination table."
                                Exit Sub
                            End If
                            mp.DestinationColumn = dest_col
                            bc.ColumnMappings.Add(mp)
                        Next
                    End If

                    bc.WriteToServer(dt)

                End Using

            End Using



        Catch ex As Exception

            err_msg = err_msg_prefix + ex.Message.ToString()

        End Try

    End Sub

    Friend Shared Function get_mssql_version(ByVal mssql_inst As String, ByVal sql_user_id As String, ByVal sql_password As String, ByRef err_msg As String) As Integer
        Try
            Dim res As Integer

            Dim cn_str As String

            'if not supplying a user id, then it uses integrated security
            If String.IsNullOrWhiteSpace(sql_user_id) Then
                cn_str = String.Format("Server = {0}; Integrated Security = True;", mssql_inst)
                log_edis_event("info", "MSSQL Version", "connection set using integrated security")
            Else
                cn_str = String.Format("Server = {0}; User ID = {1}; Password = {2};", mssql_inst, sql_user_id, sql_password)
                log_edis_event("info", "MSSQL Version", "connection set using SQL Server Authentication")
            End If

            Using cn As New SqlClient.SqlConnection(cn_str)
                cn.Open()
                Dim sql As String = "select cast(left(cast(SERVERPROPERTY('productversion') as varchar(30)),2) as int) * 10 as vsn "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
            End Using
            Return res
        Catch ex As Exception
            err_msg = "Error obtaining SQL Server version: " + ex.Message.ToString()
            Return Nothing
        End Try

    End Function

    Friend Shared Function get_cn_str_prop(cn_str As String, ByVal prop As String, ByRef err_msg As String) As String
        Dim res As String = ""
        Try

            Dim cn_parts() As String = cn_str.Split(";")

            Dim data_source_token As String = "data source"
            Dim server_token As String = "server"
            Dim user_id_token As String = "user id"
            Dim password_token As String = "password"
            Dim sspi_token As String = "integrated security"
            Dim trusted_connection_token As String = "trusted_connection"
            Dim provider_token As String = "provider"
            Dim database_token As String = "database"
            Dim initial_catalog_token As String = "initial catalog"

            Dim target_token As String
            Dim target_cn_part As String

            'validate property requested
            Select Case prop.ToUpper
                Case "DATA_SOURCE", "USER_ID", "PASSWORD", "PROVIDER", "DATABASE", "INTEGRATED_SECURITY"
                Case Else
                    err_msg = "invalid connection property [" + prop.ToUpper + "]"
                    Exit Function
            End Select


            For Each part In cn_parts

                target_cn_part = LTrim(part)

                If prop.ToUpper = "DATA_SOURCE" And Left(target_cn_part.ToLower, Len(data_source_token)) = data_source_token Then
                    target_token = data_source_token
                    Exit For
                ElseIf prop.ToUpper = "DATA_SOURCE" And Left(target_cn_part.ToLower, Len(server_token)) = server_token Then
                    target_token = server_token
                    Exit For
                ElseIf prop.ToUpper = "USER_ID" And Left(target_cn_part.ToLower, Len(user_id_token)) = user_id_token Then
                    target_token = user_id_token
                    Exit For
                ElseIf prop.ToUpper = "PASSWORD" And Left(target_cn_part.ToLower, Len(password_token)) = password_token Then
                    target_token = password_token
                    Exit For
                ElseIf prop.ToUpper = "PROVIDER" And Left(target_cn_part.ToLower, Len(provider_token)) = provider_token Then
                    target_token = provider_token
                    Exit For
                ElseIf prop.ToUpper = "DATABASE" And Left(target_cn_part.ToLower, Len(database_token)) = database_token Then
                    target_token = database_token
                    Exit For
                ElseIf prop.ToUpper = "DATABASE" And Left(target_cn_part.ToLower, Len(initial_catalog_token)) = initial_catalog_token Then
                    target_token = initial_catalog_token
                    Exit For
                ElseIf prop.ToUpper = "INTEGRATED_SECURITY" And Left(target_cn_part.ToLower, Len(sspi_token)) = sspi_token Then
                    res = "true"
                    Exit For
                ElseIf prop.ToUpper = "INTEGRATED_SECURITY" And Left(target_cn_part.ToLower, Len(trusted_connection_token)) = trusted_connection_token Then
                    res = "true"
                    Exit For
                End If
            Next

            If String.IsNullOrWhiteSpace(target_token) Then
                'token didnt exist...get out of dodge

                'for integrated security set it to false if nothing was found
                If prop.ToUpper = "INTEGRATED_SECURITY" Then
                    res = "false"
                End If

                'if the token is the data source, set error message because we were not able to get it...otherwise all other tokens are optional
                If prop.ToUpper = "DATA_SOURCE" Then
                    err_msg = "Unable to retrieve data source property from connection string"
                End If

                'get out of dodge
                Exit Function
            End If

            'parse the value
            If prop.ToUpper <> "INTEGRATED_SECURITY" Then
                res = Trim(Right(target_cn_part, Len(target_cn_part) - Len(target_token)))

                Dim equals_position As Integer = InStr("=", res) + 1

                res = Trim(Right(res, Len(res) - equals_position))

            End If

            'set integrated security to false if it wasnt found
            If prop.ToUpper = "INTEGRATED_SECURITY" And res = "" Then
                res = "false"
            End If

            Return res
        Catch ex As Exception
            err_msg = "Error obtaining SQL Server instance from connection string: " + ex.Message.ToString()
            Return Nothing
        End Try
    End Function

    Friend Shared Function get_src_cols_from_reader(ByVal reader As IDataReader, ByRef err_msg As String) As List(Of cDataTypeParser.src_column_metadata)
        Dim err_prefix As String = "Source Columns from Reader Error: "

        Dim src_cols As New List(Of cDataTypeParser.src_column_metadata)

        Try

            Dim dt As New DataTable
            dt = reader.GetSchemaTable

            For i = 0 To dt.Columns.Count - 1
                Dim src_col As New cDataTypeParser.src_column_metadata
                src_col.col_nm = dt.Columns(i).ColumnName
                src_col.data_type = dt.Columns(i).DataType.ToString
                src_col.max_len = dt.Columns(i).MaxLength
                src_cols.Add(src_col)
            Next

            Return src_cols

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString()
        End Try
    End Function


    Private Shared Function get_dest_col(src_col_nm As String, cols_list As List(Of String)) As String

        For Each col In cols_list
            'log_edis_event("info", "column list dest looper", col.ToString())
            If col.ToLower = src_col_nm.ToLower Then
                Return col.ToString
            End If
        Next

    End Function

    Friend Shared Function get_tgt_tbl_cols(ByVal mssql_inst As String, ByVal db_nm As String, ByVal schema_nm As String, ByVal tbl_nm As String, ByRef err_msg As String) As List(Of String)
        Try
            Dim cols_list As New List(Of String)

            Dim schema_sql As String = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tbl_nm AND TABLE_SCHEMA = @schema_nm"

            Dim cn_str As String = String.Format("Server = {0}; Integrated Security = True; Database = {1}", mssql_inst, db_nm)
            Using cn As New SqlClient.SqlConnection(cn_str)

                cn.Open()

                Using cmd As New SqlClient.SqlCommand(schema_sql, cn)
                    cmd.Parameters.AddWithValue("@tbl_nm", tbl_nm)
                    cmd.Parameters.AddWithValue("@schema_nm", schema_nm)
                    Using reader = cmd.ExecuteReader
                        While reader.Read
                            cols_list.Add(reader(0).ToString)
                        End While
                    End Using
                End Using

            End Using

            If cols_list.Count = 0 Then
                err_msg = "Error retrieving destination columns from table [" + db_nm + "].[" + schema_nm + "].[" + tbl_nm + "]"
            End If

            Return cols_list
        Catch ex As Exception
            err_msg = ex.Message.ToString
            Return Nothing
        End Try

    End Function

    Friend Shared Sub create_dest_tbl(ByVal mssql_inst As String, ByVal tgt_db As String, ByVal tgt_schema As String,
                                      ByVal tgt_tbl As String, ByVal src_cols As List(Of cDataTypeParser.src_column_metadata), import_all_as_text As Boolean, ByRef err_msg As String)

        Try

            'create the column list
            Dim col_list As String = "[" + src_cols(0).col_nm + "] " _
                    + get_mssql_data_type(src_cols(0), import_all_as_text)

            For i = 1 To src_cols.Count - 1
                col_list += ", [" + src_cols(i).col_nm + "]" + get_mssql_data_type(src_cols(i), import_all_as_text)
            Next

            Dim cn_str As String = String.Format("Server = {0}; Integrated Security = True; Database = {1}", mssql_inst, tgt_db)
            Using cn As New SqlConnection(cn_str)
                cn.Open()

                Dim tbl_schema As String = "[" + tgt_schema + "].[" + tgt_tbl + "]"

                Dim crt_tbl_statement As String
                If Left(tgt_tbl, 2) = "##" Then

                    Dim first_col_sql As String = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @sch AND TABLE_NAME = @tbl AND ORDINAL_POSITION = 1"

                    Using cmd As New SqlCommand(first_col_sql, cn)
                        cmd.Parameters.AddWithValue("@sch", tgt_schema)
                        cmd.Parameters.AddWithValue("@tbl", tgt_tbl)
                        crt_tbl_statement = "ALTER TABLE " + tbl_schema + " ADD " + col_list + "; ALTER TABLE " + tbl_schema + " DROP COLUMN [" + cmd.ExecuteScalar.ToString() + "]"

                    End Using

                Else
                    crt_tbl_statement = "CREATE TABLE " + tbl_schema + "( " + col_list + ")"

                End If

                Using cmd As New SqlClient.SqlCommand(crt_tbl_statement, cn)
                    cmd.ExecuteNonQuery()
                End Using

                cn.Close()
            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try



    End Sub

    Private Shared Function get_mssql_data_type(ByVal src_col As cDataTypeParser.src_column_metadata, ByVal import_as_text As Boolean) As String

        'if we just want to read as text
        If import_as_text Then
            Return "nvarchar(4000)"
            Exit Function
        End If

        Dim data_type As String = src_col.data_type
        Dim max_len As String = src_col.max_len.ToString

        Select Case data_type.ToLower
            Case "nvarchar", "varchar"
                'grow it 10%
                Dim len_buffer As Integer = max_len * 1.1
                log_edis_event("info", "create table dest data type string, length", max_len.ToString)
                If (len_buffer > 4000 AndAlso data_type.ToLower = "nvarchar") Then
                    Return data_type + "(max)"
                ElseIf (len_buffer > 8000 AndAlso data_type.ToLower = "varchar") Then
                    Return data_type + "(max)"
                Else
                    Return data_type + "(" + len_buffer.ToString + ")"
                End If
            Case "timestamp"
                Return "datetime2(7)"
            Case Else
                Return data_type
        End Select

    End Function

End Class
