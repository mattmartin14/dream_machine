'Friend Class cDB_TSFR


'    Friend Shared Sub import_server_data(src_cn_str As String, src_type As String, mssql_inst As String, src_qry As String, tgt_db As String, tgt_schema As String,
'                                         tgt_tbl As String, crt_dest_tbl As Boolean, src_pre_sql_cmd As String, batch_size As Integer, load_on_ordinal As Boolean, ByRef err_msg As String)
'        Dim err_prefix As String = "Import Server Data Error: "
'        Try

'            Dim tgt_cols As New List(Of String)

'            Dim src_cols As New List(Of cDataTypeParser.src_column_metadata)


'            'open source
'            Select Case src_type.ToLower
'                Case "odbc"
'                    Using cn As New Odbc.OdbcConnection(src_cn_str)
'                        cn.ConnectionTimeout = 0
'                        cn.Open()
'                        If Not String.IsNullOrWhiteSpace(src_pre_sql_cmd) Then
'                            Using cmd As New Odbc.OdbcCommand(src_pre_sql_cmd, cn)
'                                cmd.CommandTimeout = 0
'                                cmd.ExecuteNonQuery()
'                            End Using
'                        End If
'                        Using cmd As New Odbc.OdbcCommand(src_qry, cn)
'                            cmd.CommandTimeout = 0
'                            Using reader = cmd.ExecuteReader()
'                                If reader.HasRows Then

'                                    'load source
'                                    src_cols = cDT_Tasks.get_src_cols_from_reader(reader, err_msg)
'                                    If err_msg <> "" Then
'                                        reader.Close()
'                                        cn.Close()
'                                        cn.Dispose()
'                                        Exit Sub
'                                    End If

'                                    If crt_dest_tbl Then
'                                        cDT_Tasks.create_dest_tbl(mssql_inst, tgt_db, tgt_schema, tgt_tbl, src_cols, False, err_msg)
'                                        If err_msg <> "" Then
'                                            reader.Close()
'                                            cn.Close()
'                                            cn.Dispose()
'                                            Exit Sub
'                                        End If
'                                    End If

'                                    tgt_cols = cDT_Tasks.get_tgt_tbl_cols(mssql_inst, tgt_db, tgt_schema, tgt_tbl, err_msg)
'                                    If err_msg <> "" Then
'                                        reader.Close()
'                                        cn.Close()
'                                        cn.Dispose()
'                                        Exit Sub
'                                    End If

'                                    cDT_Tasks.bulk_insert_from_stream(mssql_inst, tgt_db, tgt_schema, tgt_tbl, tgt_cols, reader, batch_size, load_on_ordinal, err_msg)

'                                End If
'                            End Using
'                        End Using
'                    End Using
'                Case "oledb"
'                    Using cn As New OleDb.OleDbConnection(src_cn_str)
'                        'cn.ConnectionTimeout = 0
'                        cn.Open()
'                        If Not String.IsNullOrWhiteSpace(src_pre_sql_cmd) Then
'                            Using cmd As New OleDb.OleDbCommand(src_pre_sql_cmd, cn)
'                                cmd.CommandTimeout = 0
'                                cmd.ExecuteNonQuery()
'                            End Using
'                        End If
'                        Using cmd As New OleDb.OleDbCommand(src_qry, cn)
'                            cmd.CommandTimeout = 0
'                            Using reader = cmd.ExecuteReader()
'                                If reader.HasRows Then

'                                    'load source
'                                    src_cols = cDT_Tasks.get_src_cols_from_reader(reader, err_msg)
'                                    If err_msg <> "" Then
'                                        reader.Close()
'                                        cn.Close()
'                                        cn.Dispose()
'                                        Exit Sub
'                                    End If

'                                    If crt_dest_tbl Then
'                                        cDT_Tasks.create_dest_tbl(mssql_inst, tgt_db, tgt_schema, tgt_tbl, src_cols, False, err_msg)
'                                        If err_msg <> "" Then
'                                            reader.Close()
'                                            cn.Close()
'                                            cn.Dispose()
'                                            Exit Sub
'                                        End If
'                                    End If

'                                    tgt_cols = cDT_Tasks.get_tgt_tbl_cols(mssql_inst, tgt_db, tgt_schema, tgt_tbl, err_msg)
'                                    If err_msg <> "" Then
'                                        reader.Close()
'                                        cn.Close()
'                                        cn.Dispose()
'                                        Exit Sub
'                                    End If

'                                    cDT_Tasks.bulk_insert_from_stream(mssql_inst, tgt_db, tgt_schema, tgt_tbl, tgt_cols, reader, batch_size, load_on_ordinal, err_msg)
'                                End If
'                            End Using
'                        End Using
'                    End Using
'            End Select

'        Catch ex As Exception
'            err_msg = err_prefix + ex.Message.ToString()
'        End Try
'    End Sub


'End Class
