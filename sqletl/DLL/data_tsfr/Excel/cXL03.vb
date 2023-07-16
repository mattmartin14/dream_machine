Imports NPOI.HSSF.UserModel
Imports System.IO
Imports System.Data.SqlClient
Imports System.Drawing
Imports System.Windows.Forms

Friend Class cXL03

    Friend Shared Sub import_data(file_path As String, sheet_nm As String, include_headers As Boolean, tgt_db As String, tgt_schema As String,
                                 tgt_tbl As String, mssql_inst As String, load_on_ordinal As Boolean, ByVal crt_dest_tbl As Boolean,
                                  ByVal src_row_sample_cnt As Integer, ByVal import_all_as_text As Boolean, ByVal batch_size As Integer,
                                  ByVal header_rows_to_skip As Integer, ByVal include_row_id As Boolean, ByRef err_msg As String)

        Dim err_prefix As String = "XL03 Import Data Error: "
        Try

            'Bug Fix...no longer using the src_row_sample_cnt, just set it to the batch size so they are in sync
            src_row_sample_cnt = batch_size

            Dim wb As HSSFWorkbook

            Using fs As New FileStream(file_path, FileMode.Open, FileAccess.Read)
                wb = New HSSFWorkbook(fs)
            End Using

            Dim idx = wb.GetSheetIndex(sheet_nm)
            If idx = -1 Then

                Dim sheet_list As String

                For Each sheet As HSSFSheet In wb
                    sheet_list += sheet.SheetName.ToString() + vbCrLf
                Next

                err_msg = "Sheet [" + sheet_nm + "] does not exist in workbook [" + file_path + "]." + vbCrLf
                err_msg += "The following sheets are available in the workbook: " + vbCrLf
                err_msg += sheet_list

                Exit Sub
            End If


            Dim ws As HSSFSheet = wb.GetSheet(sheet_nm)


            Dim dt As New DataTable

            Dim is_first_load_call As Boolean = True
            Dim src_cols As New List(Of cDataTypeParser.src_column_metadata)
            Dim tgt_col_list As New List(Of String)

            Dim row_offset As Integer = header_rows_to_skip

            'create headers
            Dim first_row As HSSFRow = ws.GetRow(row_offset)

            Dim col_cnt As Integer = 0
            For Each cell As HSSFCell In first_row
                If include_headers Then

                    Dim cVal As String = cell.ToString
                    cVal = Replace(cVal, vbCrLf, "")
                    cVal = Replace(cVal, vbLf, "")
                    cVal = Replace(cVal, vbCr, "")

                    dt.Columns.Add(cVal, GetType(String))
                Else
                    dt.Columns.Add("COL_" + col_cnt.ToString, GetType(String))
                    col_cnt += 1
                End If
            Next

            If include_row_id Then
                dt.Columns.Add("EDIS_ROW_ID", GetType(Integer))
            End If

            'read data
            Dim row_index As Integer = 0
            Dim curr_batch As Integer = 0
            For Each row As HSSFRow In ws

                If row_index <= row_offset Then GoTo skip_row

                Dim is_blank_row As Boolean = True

                If include_headers AndAlso row_index = row_offset Then GoTo skip_row

                'If include_headers AndAlso row_index = 0 Then GoTo skip_first

                If (row_index >= 1000 AndAlso Not EDIS._is_pro_user) Then
                    err_msg = "Importing more than 1,000 rows is a pro-only feature. Please visit www.sqletl.com to purchase an EDIS Pro license."
                    Exit Sub
                End If

                Dim dr As DataRow = dt.NewRow

                'this array method is no faster than below...the below method trims the values and ignores nulls
                'dr.ItemArray = row.Cells.Select(Function(s) s.ToString()).ToArray()

                For i = 0 To dt.Columns.Count - 1

                    Dim curr_cell As NPOI.SS.UserModel.ICell = row.GetCell(i, NPOI.SS.UserModel.MissingCellPolicy.RETURN_NULL_AND_BLANK)

                    If curr_cell IsNot Nothing Then
                        Dim val As String = Trim(curr_cell.ToString())
                        If Not String.IsNullOrWhiteSpace(val) Then
                            dr(i) = val
                            is_blank_row = False
                        Else
                            dr(i) = DBNull.Value
                        End If
                    Else
                        dr(i) = DBNull.Value
                    End If
                    'cant use row.cells(i) because it skips blanks and causes an imbalance with the column alignment
                Next

                If include_row_id Then
                    dr("EDIS_ROW_ID") = row_index
                End If

                If Not is_blank_row Then
                    dt.Rows.Add(dr)
                End If

skip_row:
                row_index += 1
                curr_batch += 1
                If curr_batch >= batch_size Then

                    'is this the first call to the load?
                    If is_first_load_call Then
                        is_first_load_call = False
                        'get the source columns
                        src_cols = cDataTypeParser.parse_source_data_types(dt, src_row_sample_cnt, err_msg)
                        If err_msg <> "" Then Exit Sub
                        If crt_dest_tbl Then
                            cDT_Tasks.create_dest_tbl(mssql_inst, tgt_db, tgt_schema, tgt_tbl, src_cols, import_all_as_text, err_msg)
                            If err_msg <> "" Then Exit Sub
                        End If
                        tgt_col_list = cDT_Tasks.get_tgt_tbl_cols(mssql_inst, tgt_db, tgt_schema, tgt_tbl, err_msg)
                        If err_msg <> "" Then Exit Sub
                    End If


                    'batch_tracker.Add(batch_id, False)
                    cDT_Tasks.bulk_insert(mssql_inst, tgt_db, tgt_schema, tgt_tbl, tgt_col_list, dt, batch_size, load_on_ordinal, "", "", err_msg)
                    If err_msg <> "" Then Exit Sub

                    dt.Clear()
                    curr_batch = 0
                End If

            Next

            'send last batch
            If curr_batch >= 1 Then
                If is_first_load_call Then
                    is_first_load_call = False
                    'get the source columns
                    src_cols = cDataTypeParser.parse_source_data_types(dt, src_row_sample_cnt, err_msg)
                    If err_msg <> "" Then Exit Sub
                    If crt_dest_tbl Then
                        cDT_Tasks.create_dest_tbl(mssql_inst, tgt_db, tgt_schema, tgt_tbl, src_cols, import_all_as_text, err_msg)
                        If err_msg <> "" Then Exit Sub
                    End If
                    tgt_col_list = cDT_Tasks.get_tgt_tbl_cols(mssql_inst, tgt_db, tgt_schema, tgt_tbl, err_msg)
                    If err_msg <> "" Then Exit Sub
                End If

                cDT_Tasks.bulk_insert(mssql_inst, tgt_db, tgt_schema, tgt_tbl, tgt_col_list, dt, batch_size, load_on_ordinal, "", "", err_msg)
                If err_msg <> "" Then Exit Sub
            End If

            dt = Nothing

            'back off row count for headers
            If include_headers Then row_index -= 1

            'back off skipped rows
            row_index -= header_rows_to_skip

            EDIS.log_rows_tsfr(row_index)

            'Console.WriteLine("rows imported: {0}", row_index)

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString

            If Main.show_call_stack Then
                err_msg += ex.StackTrace.ToString()
            End If

        End Try


    End Sub

    Friend Shared Sub get_sheet_list(ByVal file_path As String, ByVal mssql_inst As String,
            ByVal output_tbl_nm As String, ByRef err_msg As String
        )
        Dim err_prefix As String = "Excel Get Sheet List Error: "

        Try

            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                For Each sheet As HSSFSheet In wb
                    Dim sql As String = "INSERT [" + output_tbl_nm + "] VALUES (@sheet_nm)"
                    Using cmd As New SqlClient.SqlCommand(sql, cn)
                        cmd.Parameters.AddWithValue("@sheet_nm", sheet.SheetName.ToString)
                        cmd.ExecuteNonQuery()
                    End Using
                Next
            End Using

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub

    Friend Shared Sub clear_worksheet(ByVal file_path As String, ByVal sheet_nm As String, ByRef err_msg As String)

        Dim err_prefix As String = "Excel Clear Worksheet Error: "

        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            Dim idx = wb.GetSheetIndex(sheet_nm)
            If idx = -1 Then
                err_msg = "Sheet [" + sheet_nm + "] does not exist"
                Exit Sub
            End If

            'drop/recreate is much faster, but if you drop/replace, the cell formula references get screwed up
            'wb.RemoveSheetAt(idx)
            'wb.CreateSheet(_sheet_nm)
            'wb.SetSheetOrder(_sheet_nm, idx)
            'wb.SetActiveSheet(idx)

            Dim sheet = wb.GetSheet(sheet_nm)
            For Each row As HSSFRow In sheet
                row.RemoveAllCells()
            Next

            'sheet.SetAutoFilter(Nothing)

            'sheet.SetAutoFilter(New NPOI.SS.Util.CellRangeAddress())
            'New NPOI.SS.Util.CellRangeAddress(0, _row_index - 1, 0, _total_columns - 1)



            'reopen the stream and write it out
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub

    Friend Shared Sub rename_worksheet(file_path As String, sheet_nm As String, new_sheet_nm As String, ByRef err_msg As String)

        Dim err_prefix As String = "Excel Rename Worksheet Error: "
        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            'Does the current sheet exist?
            Dim idx = wb.GetSheetIndex(sheet_nm)
            If idx = -1 Then
                err_msg = "Sheet [" + sheet_nm + "] does not exist"
                Exit Sub

            End If

            'Does the target sheet already exist?
            Dim idx1 = wb.GetSheetIndex(new_sheet_nm)
            If idx1 <> -1 Then
                err_msg = "Sheet [" + new_sheet_nm + "] already exists. You cannot have two worksheets with the same name."
                Exit Sub
            End If

            'rename it 
            wb.SetSheetName(wb.GetSheetIndex(sheet_nm), new_sheet_nm)

            'reopen the stream and write it out
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub

    Friend Shared Sub add_worksheet(ByVal file_path As String, ByVal sheet_nm As String, ByVal err_msg As String)
        Dim err_prefix As String = "Excel Add Worksheet Error: "
        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
                'after the new hssfworkbook event, it closes the file stream
            End Using

            'add sheet
            Dim idx = wb.GetSheetIndex(sheet_nm)
            If idx <> -1 Then
                err_msg = "Sheet [" + sheet_nm + "] already exists"
                Exit Sub
            End If

            wb.CreateSheet(sheet_nm)

            'reopen the stream and write it out
            Using fstream As New FileStream(file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try
    End Sub

    Shared Sub export_data(ByVal file_path As String, ByVal mssql_inst As String, ByVal src_qry As String, ByVal sheet_nm As String,
                           ByVal include_headers As Boolean, ByVal bold_headers As Boolean,
                           ByVal autofit_cols As Boolean, ByVal apply_autofilter As Boolean, ByRef err_msg As String
        )
        Dim err_prefix As String = "Excel Data Export Error: "

        Try

            Dim xl_row_limit As Integer = 65536

            Dim wb As HSSFWorkbook

            Dim column_meta As New List(Of export_column)

            'if the file is already there, read in the stream
            If File.Exists(file_path) Then
                Using fstream As New FileStream(file_path, FileMode.Open)
                    wb = New HSSFWorkbook(fstream)
                    'after the new hssfworkbook event, it closes the file stream

                    'if no sheet add it
                    Dim idx = wb.GetSheetIndex(sheet_nm)
                    If idx = -1 Then
                        wb.CreateSheet(sheet_nm)
                    End If

                End Using
            Else
                'create the workbook and add a sheet
                wb = New HSSFWorkbook()
                wb.CreateSheet(sheet_nm)
            End If

            'set the data sheet
            Dim data_sheet As HSSFSheet = wb.GetSheet(sheet_nm)

            'XLS does not support table styles

            Dim fFam As New System.Drawing.FontFamily("Calibri")
            Dim wb_font As New System.Drawing.Font(fFam, 11)

            Dim buffer_for_auto_filter As Boolean = False
            If apply_autofilter Then
                buffer_for_auto_filter = True
            End If

            'Write data

            Dim _total_columns As Integer

            'NPOI starts at base 0 for columns and rows
            Dim _row_index As Integer = 0
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()


                Using cmd As New SqlClient.SqlCommand(src_qry, cn)
                    cmd.CommandTimeout = 0

                    Using reader = cmd.ExecuteReader

                        _total_columns = reader.FieldCount

                        Dim font = wb.CreateFont()
                        font.Boldweight = NPOI.SS.UserModel.FontBoldWeight.Bold
                        font.FontName = "Calibri"
                        font.FontHeightInPoints = 11

                        '---------------------------------------------------------------------------------------------------------------
                        'add headers

                        If include_headers Then

                            Dim r As HSSFRow = data_sheet.CreateRow(_row_index)

                            For _col_index = 0 To _total_columns - 1

                                Dim col_nm As String = reader.GetName(_col_index)

                                Dim col As New export_column With {.ord_pos = _col_index.ToString, .col_width = i_get_cell_val_width(wb_font, col_nm, buffer_for_auto_filter), .col_nm = col_nm}
                                column_meta.Add(col)

                                '_export_col_widths.Add(col_nm, i_get_cell_val_width(wb_font, col_nm, buffer_for_auto_filter))

                                If bold_headers Then
                                    Dim cell = r.CreateCell(_col_index, NPOI.SS.UserModel.CellType.String)
                                    cell.SetCellValue(reader.GetName(_col_index))
                                    cell.CellStyle = wb.CreateCellStyle
                                    cell.CellStyle.SetFont(font)
                                Else
                                    r.CreateCell(_col_index, NPOI.SS.UserModel.CellType.String).SetCellValue(reader.GetName(_col_index))
                                End If


                            Next

                            _row_index += 1

                        End If

                        '----------------------------------------------------------------------------------------------------------
                        'Write the data

                        While reader.Read

                            If _row_index >= xl_row_limit Then
                                err_msg = "Excel XLS documents can only contain a maximum 65,536 rows per worksheet. Your query exceeds that amount."
                                reader.Close()
                                cn.Close()
                                cn.Dispose()
                                cmd.Dispose()
                                Exit Sub
                            End If

                            If (_row_index >= 1000 AndAlso Not EDIS._is_pro_user) Then
                                err_msg = "Exporting more than 1,000 rows is a pro-only feature. Please visit www.sqletl.com to purchase an EDIS Pro license."
                                Exit Sub
                            End If

                            'add the row
                            Dim r As HSSFRow = data_sheet.CreateRow(_row_index)

                            'add the fields
                            For _col_index = 0 To _total_columns - 1

                                'for first row, add to dictionary
                                If Not include_headers And _row_index = 0 Then
                                    'populate the dictionary
                                    Dim c_width As Double = i_get_cell_val_width(wb_font, reader(_col_index).ToString, buffer_for_auto_filter)

                                    Dim col As New export_column With {.ord_pos = _col_index.ToString, .col_width = c_width, .col_nm = "COL_" + _col_index.ToString}
                                    column_meta.Add(col)

                                    '_export_col_widths.Add(_col_index.ToString, i_get_cell_val_width(wb_font, reader(_col_index).ToString, buffer_for_auto_filter))
                                End If

                                Dim data_type As String = reader.GetDataTypeName(_col_index).ToString.ToLower

                                write_cell(r, data_type, reader, _col_index, wb)

                                'evaluate the width
                                If _row_index <= 100 Then
                                    ' Dim curr_catalog_row_width As Double = _export_col_widths.Item(reader.GetName(_col_index))
                                    Dim curr_catalog_row_width As Double = column_meta.Item(_col_index).col_width

                                    Dim this_row_width As Double = i_get_cell_val_width(wb_font, reader(_col_index).ToString, buffer_for_auto_filter)

                                    If this_row_width > curr_catalog_row_width Then

                                        column_meta.Item(_col_index).col_width = this_row_width

                                        '_export_col_widths.Item(reader.GetName(_col_index)) = this_row_width
                                    End If

                                End If

                            Next

                            _row_index += 1
                        End While
                    End Using

                End Using
            End Using

            If apply_autofilter Then
                data_sheet.SetAutoFilter(New NPOI.SS.Util.CellRangeAddress(0, _row_index - 1, 0, _total_columns - 1))
            End If

            'this way is very slow on large data sets
            'If _autofit_columns Then
            '    For i = 0 To _total_columns - 1
            '        data_sheet.AutoSizeColumn(i)
            '    Next
            'End If


            If autofit_cols Then
                Dim j As Integer = 0
                For Each col In column_meta

                    'the units seem very jacked up for how they do this
                    '250 seems to work...go figure
                    Dim final_val = col.col_width * 250


                    data_sheet.SetColumnWidth(j, CInt(final_val))
                    j += 1
                Next
            End If

            Dim xls_file As New FileStream(file_path, FileMode.OpenOrCreate)

            wb.Write(xls_file)
            xls_file.Close()

            'back off the row index 1 since we auto incriment after each row
            EDIS.log_rows_tsfr(_row_index - 1)
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub
    Private Class export_column
        Property ord_pos As String
        Property col_width As Double
        Property col_nm As String
    End Class
    Private Shared Function i_get_cell_val_width(font_nm As System.Drawing.Font, cell_val As String, using_auto_filter As Boolean) As Double


        Dim autoFilter_icon_buffer
        If using_auto_filter Then
            autoFilter_icon_buffer = 2.5D
        Else
            autoFilter_icon_buffer = 0D
        End If

        Dim text_size As Size = TextRenderer.MeasureText(cell_val, font_nm)
        Dim width As Double = CDbl(((text_size.Width / CDbl(7)) * 256) - (128 / 7)) / 256
        width = CDbl(Decimal.Round(CDec(width) + 0.2D + autoFilter_icon_buffer, 2))

        'cannot exceed 255...checking at 254 for rounding
        If width > 254 Then
            width = 255
        End If

        Return width

    End Function

    Private Shared Sub write_cell(row As HSSFRow, format_type As String, reader As SqlDataReader, index As Integer, wb As HSSFWorkbook)


        If Not reader.IsDBNull(index) Then
            Select Case format_type.ToLower

                Case "int"
                    row.CreateCell(index).SetCellValue(reader.GetInt32(index))
                Case "bigint"
                    row.CreateCell(index).SetCellValue(reader.GetInt64(index))
                Case "smallint", "tinyint"
                    row.CreateCell(index).SetCellValue(reader.GetInt16(index))
                Case "float"
                    row.CreateCell(index).SetCellValue(reader.GetDouble(index))
                Case "numeric", "decimal", "money"
                    row.CreateCell(index).SetCellValue(reader.GetDecimal(index))
                Case "date"
                    row.CreateCell(index).SetCellValue(reader.GetDateTime(index).ToShortDateString())
                    'Dim cell As HSSFCell = row.CreateCell(index)

                    'Dim cellStyle As HSSFCellStyle = wb.CreateCellStyle()

                    'cell.CellStyle.s

                    'cell.SetCellValue(reader.GetDateTime(index))

                    'cellStyle.DataFormat = "mm/dd/yy"
                Case Else
                    'write as string
                    row.CreateCell(index).SetCellValue(reader(index).ToString)
            End Select
        End If


    End Sub

End Class
