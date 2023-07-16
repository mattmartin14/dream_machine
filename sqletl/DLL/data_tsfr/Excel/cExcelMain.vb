Friend Class cExcelMain

    Shared Sub main(ByRef err_msg As String)

        Dim file_path As String = EDIS.get_edis_pval("file_path")

        Dim isXL03 As Boolean = False
        If Right(file_path, 4) = ".xls" Then
            isXL03 = True
        End If

        Dim sheet_nm As String = EDIS.get_edis_pval("sheet_nm")

        Dim sub_task As String = EDIS.get_edis_pval("sub_task").ToLower

        Select Case sub_task
            Case "export_data"
                Dim src_qry As String = EDIS.get_edis_pval("src_qry")
                Dim include_headers As Boolean = CBool(EDIS.get_edis_pval("include_headers"))
                Dim bold_headers As Boolean = CBool(EDIS.get_edis_pval("bold_headers"))
                Dim apply_autofilter As Boolean = CBool(EDIS.get_edis_pval("use_autofilter"))
                Dim autofit_cols As Boolean = CBool(EDIS.get_edis_pval("autofit_cols"))
                If isXL03 Then
                    cXL03.export_data(file_path, EDIS._mssql_inst, src_qry, sheet_nm, include_headers, bold_headers, autofit_cols, apply_autofilter, err_msg)
                Else
                    cXL07.export_data(file_path, EDIS._mssql_inst, src_qry, sheet_nm, include_headers, bold_headers, autofit_cols, apply_autofilter, err_msg)
                End If
            Case "import_data"
                Dim include_headers As Boolean = CBool(EDIS.get_edis_pval("include_headers"))
                Dim tgt_db As String = EDIS.get_edis_pval("tgt_db")
                Dim tgt_schema As String = EDIS.get_edis_pval("tgt_schema")
                Dim tgt_tbl As String = EDIS.get_edis_pval("tgt_tbl")
                Dim load_on_ordinal As Boolean = CBool(EDIS.get_edis_pval("load_on_ordinal"))
                Dim crt_dest_tbl As Boolean = CBool(EDIS.get_edis_pval("crt_dest_tbl"))
                Dim src_row_sample_cnt As Integer = CInt(EDIS.get_edis_pval("src_row_sample_scan_cnt"))
                Dim import_all_as_text As Boolean = CBool(EDIS.get_edis_pval("import_all_as_text"))
                Dim batch_size As Integer = CInt(EDIS.get_edis_pval("batch_size"))
                Dim header_rows_to_skip As Integer = CInt(EDIS.get_edis_pval("header_rows_to_skip"))
                Dim include_row_id As Boolean = CBool(EDIS.get_edis_pval("include_row_id"))

                If isXL03 Then
                    cXL03.import_data(file_path, sheet_nm, include_headers, tgt_db, tgt_schema, tgt_tbl, EDIS._mssql_inst, load_on_ordinal, crt_dest_tbl,
                                      src_row_sample_cnt, import_all_as_text, batch_size, header_rows_to_skip, include_row_id, err_msg)
                Else
                    cXL07.import_data(file_path, sheet_nm, include_headers, tgt_db, tgt_schema, tgt_tbl, EDIS._mssql_inst, load_on_ordinal, crt_dest_tbl,
                                      src_row_sample_cnt, import_all_as_text, batch_size, header_rows_to_skip, include_row_id, err_msg)
                End If

            Case "clear_worksheet"
                If isXL03 Then
                    cXL03.clear_worksheet(file_path, sheet_nm, err_msg)
                Else
                    cXL07.clear_worksheet(file_path, sheet_nm, err_msg)
                End If
            Case "rename_worksheet"
                Dim new_sheet_nm As String = EDIS.get_edis_pval("new_sheet_nm")
                If isXL03 Then
                    cXL03.rename_worksheet(file_path, sheet_nm, new_sheet_nm, err_msg)
                Else
                    cXL07.rename_worksheet(file_path, sheet_nm, new_sheet_nm, err_msg)
                End If
            Case "add_worksheet"
                If isXL03 Then
                    cXL03.add_worksheet(file_path, sheet_nm, err_msg)
                Else
                    cXL07.add_worksheet(file_path, sheet_nm, err_msg)
                End If
            Case "get_sheet_list"
                Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                If isXL03 Then
                    cXL03.get_sheet_list(file_path, EDIS._mssql_inst, output_tbl_nm, err_msg)
                Else
                    cXL07.get_sheet_list(file_path, EDIS._mssql_inst, output_tbl_nm, err_msg)
                End If

        End Select

    End Sub

End Class
