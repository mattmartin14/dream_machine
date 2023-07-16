
Imports System.Data.OleDb
Imports System.Data.Odbc
Imports System.Data.SqlClient
Imports System.Collections.Generic

Module mSql_Cmd

    Sub Sql_Cmd_main(ByRef err_msg As String)

        'handle the connection string
        Dim cn_str As String = EDIS.get_edis_pval("cn_str")
        'Dim src_sys_nm As String = EDIS.get_edis_pval("src_sys_nm").ToLower
        'If src_sys_nm = "excel" Or src_sys_nm = "access" Then
        '    cn_str = ""
        'Else
        '    cn_str = EDIS.get_sensitive_value("cn_str", src_sys_nm)
        '    If EDIS._edis_err_msg <> "" Then Exit Sub
        'End If
        'EDIS.edis_params.Add("cn_str", cn_str)

        'edis_params.Add("cn_str", get_sensitive_value("cn_str", get_edis_pval("src_sys_nm ")))

        Select Case LCase(EDIS.get_edis_pval("sub_task"))
            Case "run_sql_cmd"
                Call run_sql_cmd(err_msg)
            Case "get_scalar"
                Call get_scalar(EDIS.get_edis_pval("cn_str"),
                                 EDIS.get_edis_pval("sql_qry"),
                                 EDIS.get_edis_pval("cn_provider"),
                                 EDIS.get_edis_pval("cn_sub_provider"),
                                 EDIS.get_edis_pval("mssql_inst"),
                                 EDIS.get_edis_pval("tmp_tbl_nm"),
                                 err_msg
                                )
        End Select

    End Sub

    Private Sub run_sql_cmd(ByRef err_msg As String)
        Try

            Dim cn_str As String = EDIS.get_edis_pval("cn_str")
            Dim sql_cmd_txt As String = EDIS.get_edis_pval("sql_cmd")
            Dim cn_provider As String = EDIS.get_edis_pval("cn_provider")
            Dim cn_sub_provider As String = EDIS.get_edis_pval("cn_sub_provider")

            'ado.net conversion
            If LCase(cn_provider) = "ado.net" Then
                cn_provider = LCase(cn_sub_provider)
            End If

            'split all sql commands if multiple were sent
            Dim sql_cmds As String() = sql_cmd_txt.Split(";")

            Select Case LCase(cn_provider)
                Case "oledb"
                    'excel/access handler
                    Dim server_platform As String = EDIS.get_edis_pval("server_platform").ToUpper
                    If server_platform = "EXCEL" Or server_platform = "ACCESS" Then
                        Dim file_path As String = EDIS.get_edis_pval("file_path").ToUpper
                        cn_str = mCn_builder.build_cn_str(server_platform, file_path, False, True)
                    End If

                    Using cn As New OleDbConnection(cn_str)
                        cn.Open()

                        For Each cmd_txt In sql_cmds
                            If Not String.IsNullOrWhiteSpace(cmd_txt) Then
                                'If Not is_just_empty_space(cmd_txt) Then
                                Using cmd As New OleDbCommand(cmd_txt, cn)
                                    cmd.CommandTimeout = 0
                                    cmd.ExecuteNonQuery()
                                End Using
                            End If
                        Next
                        cn.Close()
                    End Using
                Case "odbc"
                    Using cn As New OdbcConnection(cn_str)
                        cn.Open()

                        For Each cmd_txt In sql_cmds
                            If Not String.IsNullOrWhiteSpace(cmd_txt) Then
                                'If Not is_just_empty_space(cmd_txt) Then
                                Using cmd As New OdbcCommand(cmd_txt, cn)
                                    cmd.CommandTimeout = 0
                                    cmd.ExecuteNonQuery()
                                End Using
                            End If
                        Next
                        cn.Close()
                    End Using
                Case Else
                    err_msg = "The provider you are using ['" + cn_provider + "'] is not supported. Please use OLEDB, ODBC, or ADO.NET"
            End Select

        Catch ex As Exception
            err_msg = ex.Message
        End Try

    End Sub

    'Private Function is_just_empty_space(sql_cmd As String) As Boolean
    '    Dim new_str As String = sql_cmd
    '    new_str = new_str.Replace(vbCrLf, "")
    '    new_str = new_str.Replace(vbCr, "")
    '    new_str = new_str.Replace(vbLf, "")
    '    new_str = new_str.Replace(vbTab, "")

    '    If Trim(new_str) = "" Then
    '        Return True
    '    Else
    '        Return False
    '    End If

    'End Function

    'Private Function has_sql_cmd_txt(sql_cmd As String) As Boolean
    '    Dim res As Boolean = False
    '    Select Case True
    '        Case sql_cmd.ToLower Like "*create*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*exec*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*insert*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*update*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*delete*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*drop*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*alter*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*merge*"
    '            res = True
    '        Case sql_cmd.ToLower Like "*select*into*"
    '            res = True
    '    End Select

    '    Return res
    'End Function

    Private Sub get_scalar(ByVal cn_str As String, _
                           ByVal sql_cmd As String, _
                           ByVal cn_provider As String, _
                           ByVal cn_sub_provider As String, _
                           ByVal mssql_inst As String, _
                           ByVal tmp_tbl_nm As String, _
                           ByRef err_msg As String _
                        )
        Try

            'ado.net conversion
            If LCase(cn_provider) = "ado.net" Then
                cn_provider = LCase(cn_sub_provider)
            End If

            Dim res As String = ""
            Select Case LCase(cn_provider)
                Case "oledb"
                    Using cn As New OleDb.OleDbConnection(cn_str)
                        cn.Open()
                        Using cmd As New OleDb.OleDbCommand(sql_cmd, cn)
                            cmd.CommandTimeout = 0

                            Dim var = cmd.ExecuteScalar()
                            If Not var Is Nothing Then
                                res = var.ToString()
                            End If

                            'res = cmd.ExecuteScalar().ToString
                        End Using
                    End Using
                Case "odbc"
                    Using cn As New OdbcConnection(cn_str)
                        cn.Open()
                        Using cmd As New OdbcCommand(sql_cmd, cn)
                            cmd.CommandTimeout = 0

                            Dim var2 = cmd.ExecuteScalar()
                            If Not var2 Is Nothing Then
                                res = var2.ToString()
                            End If

                            'res = cmd.ExecuteScalar().ToString
                        End Using
                    End Using

                Case Else
                    err_msg = "The provider you are using ['" + cn_provider + "'] is not supported. Please use OLEDB, ODBC, or ADO.NET"
                    Exit Sub
            End Select

            If res = "" Then Exit Sub

            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                Dim sql_str As String = "insert [" + tmp_tbl_nm + "] values (@val)"
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                    cmd.CommandTimeout = 0
                    cmd.Parameters.AddWithValue("@val", res)
                    cmd.ExecuteNonQuery()
                End Using
            End Using

        Catch ex As Exception
            err_msg = ex.Message
        End Try

    End Sub

End Module