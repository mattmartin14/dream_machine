
Imports System.Data.OleDb

Class cExcel_Functions
    Inherits data_tsfr

    Shared Function get_sheet_list(ByRef err_msg As String, ByVal xl_file_path As String) As String()

        Dim res() As String

        Try
            Dim include_headers As Boolean = CBool(EDIS.get_edis_pval("include_headers"))

            'get the connection string
            Dim cn_str As String = mCn_builder.build_cn_str("EXCEL", xl_file_path, False, include_headers)

            Using cn As New OleDb.OleDbConnection(cn_str)

                cn.Open()

                Dim dt As DataTable = cn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, Nothing)

                Dim sub_res(dt.Rows.Count) As String
                Dim sheet_cnt As Integer = 0

                ' the table listing has both internal sheets and worksheets
                '       worksheets have a $ on the end, so we need to loop twice
                For i As Integer = 0 To dt.Rows.Count - 1
                    sub_res(i) = dt.Rows(i).Item("Table_Name").ToString()
                    If Right(sub_res(i), 1) = "$" Then sheet_cnt += 1
                Next i

                ReDim res(sheet_cnt)

                Dim k As Integer = 0
                For i As Integer = 0 To dt.Rows.Count - 1
                    If Right(dt.Rows(i).Item("Table_Name").ToString(), 1) = "$" Then
                        res(k) = dt.Rows(i).Item("Table_Name").ToString()
                        k += 1
                    End If
                Next i

            End Using

            Return res
        Catch ex As Exception
            err_msg = ex.Message.ToString
            Return Nothing
        End Try

        Return res

    End Function

    Shared Function get_excel_template_temp_path(ByRef err_msg As String) As String
        Dim res As String = ""
        Try
            Dim file_path As String = EDIS.get_edis_pval("file_path")
            Dim xl_ext As String = ""
            If LCase(Right(file_path, 4)) = "xlsx" Then
                xl_ext = "xlsx"
            ElseIf (Right(file_path, 4)) = ".xls" Then
                xl_ext = "xls"
            End If

            Dim tmp_path As String = IO.Path.GetDirectoryName(file_path) + "\" + Replace(Guid.NewGuid().ToString(), "-", "_") + "." + xl_ext
            res = tmp_path
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

        Return res

    End Function

    'creates the table on an existing xl workbook
    Shared Sub create_xl_table(ByVal cn_str As String, ByVal dest_tbl As String, ByVal src_meta_coll As Collection, _
                                ByRef err_msg As String)

        Try

            Dim xl_crt_tbl_cmd As String = "CREATE TABLE `" + dest_tbl + "` ( "

            For i As Integer = 1 To src_meta_coll.Count

                Dim col As src_meta = src_meta_coll(i)

                Dim dest_xl_typ As String = dft_functions.data_type_translator(col.data_type_str, "EXCEL")

                If i > 1 Then xl_crt_tbl_cmd += ", "

                Select Case LCase(dest_xl_typ)
                    Case "longtext"
                        xl_crt_tbl_cmd += "`" + col.col_nm + "`" + " longtext "
                    Case "memo"
                        xl_crt_tbl_cmd += "`" + col.col_nm + "`" + " memo "
                    Case "long"
                        xl_crt_tbl_cmd += "`" + col.col_nm + "`" + " long "
                    Case "datetime"
                        xl_crt_tbl_cmd += "`" + col.col_nm + "`" + " datetime "
                    Case Else
                        xl_crt_tbl_cmd += "`" + col.col_nm + "`" + " longtext "
                End Select

            Next i

            xl_crt_tbl_cmd += ")"
            dft_functions.run_oledb_cmd(xl_crt_tbl_cmd, cn_str, err_msg)

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub



    Shared Sub create_xl_template(ByVal temp_path As String, ByVal include_headers As Boolean, ByVal dest_tbl As String, _
                                  ByRef err_msg As String, ByVal src_meta_coll As Collection, ByRef xl_cols_list As String)

        'xl sheet names "template" and "template_blank"
        Try
            Dim xl_ext As String = ""
            If LCase(Right(temp_path, 4)) = "xlsx" Then
                xl_ext = "xlsx"
            ElseIf (Right(temp_path, 4)) = ".xls" Then
                xl_ext = "xls"
            End If

            write_ms_template_file_out(xl_ext, temp_path, err_msg)

            'build select into statement
            Dim xl_sql_sel_into_cmd As String = "SELECT "

            For i As Integer = 1 To src_meta_coll.Count

                Dim col As src_meta = src_meta_coll(i)

                Dim dest_xl_typ As String = dft_functions.data_type_translator(col.data_type_str, "EXCEL")

                If i > 1 Then xl_sql_sel_into_cmd += ", "

                Select Case LCase(dest_xl_typ)
                    Case "longtext"
                        xl_sql_sel_into_cmd += "text_col as "
                    Case "memo"
                        xl_sql_sel_into_cmd += "memo_col as "
                    Case "long"
                        xl_sql_sel_into_cmd += "int_col as "
                    Case "datetime"
                        xl_sql_sel_into_cmd += "date_col as "
                    Case Else
                        xl_sql_sel_into_cmd += "text_col as "
                End Select
                xl_sql_sel_into_cmd += "`" + col.col_nm + "`"

                If i > 1 Then xl_cols_list += ", "
                xl_cols_list += "`" + col.col_nm + "`"

            Next i

            xl_sql_sel_into_cmd += ", mddt_edis$$_marker_col INTO [" + dest_tbl + "] FROM [template$]"

            'get the connection string
            Dim cn_str As String = mCn_builder.build_cn_str("EXCEL", temp_path, False, include_headers)
            dft_functions.run_oledb_cmd(xl_sql_sel_into_cmd, cn_str, err_msg)

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Shared Sub send_xl_sheet_to_final_path(ByRef err_msg As String, ByVal temp_path As String, ByVal does_sheet_exist As Boolean, ByVal xl_cols_list As String,
                                           Optional ByRef xl_cmd_str As String = "", Optional ByVal just_cmd_string As Boolean = False
                        )

        Try

            Dim file_path As String = EDIS.get_edis_pval("file_path")
            Dim dest_tbl As String = EDIS.get_edis_pval("dest_tbl")
            Dim include_headers As Boolean = CBool(EDIS.get_edis_pval("include_headers"))


            Dim xl_type As String = ""

            If Right(file_path, 4) = "xlsx" Then
                xl_type = "xlsx"
            ElseIf Right(file_path, 4) = ".xls" Then
                xl_type = "xls"
            End If

            Select Case LCase(xl_type)
                Case "xlsx"
                    xl_type = "Excel 12.0 XML"
                Case "xls"
                    xl_type = "Excel 8.0"
            End Select

            Dim dest_tbl_with_dollar As String, dest_tbl_without_dollar As String
            If Right(dest_tbl, 1) = "$" Then
                dest_tbl_with_dollar = dest_tbl
                dest_tbl_without_dollar = Left(dest_tbl, Len(dest_tbl) - 1)
            Else
                dest_tbl_with_dollar = dest_tbl + "$"
                dest_tbl_without_dollar = dest_tbl

            End If

            Dim xl_cmd As String

            If does_sheet_exist = False Then
                xl_cmd =
                    "SELECT " + xl_cols_list + " INTO [" + xl_type + ";Database=" + file_path + "].[" + dest_tbl_without_dollar + "] " +
                    "FROM [" + dest_tbl_with_dollar + "] " +
                    "WHERE mddt_edis$$_marker_col IS NULL"
            Else
                'appending doesn't work
                xl_cmd =
                    "INSERT INTO [" + dest_tbl_with_dollar + "] (" + xl_cols_list + ") IN '" + file_path + "' '" + xl_type + ";' " +
                    "SELECT " + xl_cols_list + " FROM [" + dest_tbl_without_dollar + "] " +
                    "WHERE mddt_edis$$_marker_col IS NULL "
            End If

            If just_cmd_string = True Then
                xl_cmd_str = xl_cmd
                Exit Sub
            End If


            Dim cn_str As String = mCn_builder.build_cn_str("EXCEL", temp_path, False, include_headers)

            dft_functions.run_oledb_cmd(xl_cmd, cn_str, err_msg)
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try


    End Sub

    Shared Sub write_ms_template_file_out(ByVal ms_file_type As String, ByVal file_path As String, ByRef err_msg As String)
        Try
            Select Case LCase(ms_file_type)
                Case "xlsx"
                    System.IO.File.WriteAllBytes(file_path, My.Resources.Book1_xlsx)
                Case "xls"
                    System.IO.File.WriteAllBytes(file_path, My.Resources.Book1_xls)
                Case "accdb"
                    System.IO.File.WriteAllBytes(file_path, My.Resources.Database_accdb)
                Case "mdb"
                    System.IO.File.WriteAllBytes(file_path, My.Resources.Database_mdb)
            End Select
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

End Class
