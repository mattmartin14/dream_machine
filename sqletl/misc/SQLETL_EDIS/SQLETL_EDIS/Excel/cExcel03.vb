
Imports System.Text
Imports System.IO
Imports System.Drawing
Imports NPOI.HSSF.UserModel
Imports System.Data.SqlClient
Imports System.Windows.Forms

'https://www.codeproject.com/articles/33850/generate-excel-files-without-using-microsoft-excel

'This is cool but at the end of the day, use NPOI
'http://npoi.codeplex.com/SourceControl/latest#NPOI/HSSF/UserModel/HSSFObjectData.cs

'exmaples of npoi: http://poi.apache.org/spreadsheet/quick-guide.html#NewWorkbook

'----------------------------------------------------------------------------------------
'Functions: 
'1: export data - Done
'2: Add Worksheet - done
'3: Rename Worksheet - done
'4: Clear Worksheet - done
'5: Get Sheet List - Done
'----------------------------------------------------------------------------------------
'In Dev:

'1: Import Data
'2: Delete Sheet



Friend Class cExcel03
    Private Shared _xl_err_msg As String
    Private Shared _writer As BinaryWriter
    'Private Shared _export_col_widths As New Dictionary(Of String, Double)
    Private Shared _worksheets As New Dictionary(Of String, String)
    Private Shared _file_path As String = cParams.get_param_val("file_path")
    Private Shared _sheet_nm As String = cParams.get_param_val("sheet_nm")
    Private Shared _new_sheet_nm As String = cParams.get_param_val("net_sheet_nm")
    Private Shared _mssql_inst As String = cParams.get_param_val("mssql_inst")
    Private Shared _output_tbl_nm As String = cParams.get_param_val("output_tbl_nm")
    Private Shared _qry As String = cParams.get_param_val("src_qry")
    Private Shared _include_headers As String = cParams.get_param_val("include_headers")
    Private Shared _bold_headers As String = cParams.get_param_val("bold_headers")
    Private Shared _autofit_columns As String = cParams.get_param_val("autofit_cols")
    Private Shared _include_autofilter As String = cParams.get_param_val("use_autofilter")

    Private Class export_column
        Property ord_pos As String
        Property col_width As Double
        Property col_nm As String
    End Class

    Shared Sub main(ByRef err_msg As String)
        Try
            Dim sub_task As String = cParams.get_param_val("sub_task")
            Select Case sub_task
                Case "export_data"
                    export_excel_03_data()
                Case "get_sheet_list"
                    get_sheet_list()
                Case "add_worksheet"
                    add_worksheet()
                Case "rename_worksheet"
                    rename_worksheet()
                Case "clear_worksheet"
                    clear_worksheet()

                Case Else
                    Throw New Exception("task [" + sub_task + "] for Excel 03 task set does not exist")
            End Select

            err_msg = _xl_err_msg

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub


    Private Shared Sub get_sheet_list()

        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
                cn.Open()
                For Each sheet As HSSFSheet In wb
                    Dim sql As String = "INSERT [" + _output_tbl_nm + "] VALUES (@sheet_nm)"
                    Using cmd As New SqlClient.SqlCommand(sql, cn)
                        cmd.Parameters.AddWithValue("@sheet_nm", sheet.SheetName.ToString)
                        cmd.ExecuteNonQuery()
                    End Using
                Next
            End Using

        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try


    End Sub

    Private Shared Sub delete_worksheet()

        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            Dim idx = wb.GetSheetIndex(_sheet_nm)
            If idx = -1 Then
                Throw New Exception("Sheet [" + _sheet_nm + "] does not exist")
            End If

            wb.RemoveSheetAt(idx)

            'reopen the stream and write it out
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try



    End Sub

    Private Shared Sub clear_worksheet()

        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            Dim idx = wb.GetSheetIndex(_sheet_nm)
            If idx = -1 Then
                Throw New Exception("Sheet [" + _sheet_nm + "] does not exist")
            End If

            'drop/recreate is much faster, but if you drop/replace, the cell formula references get screwed up
            'wb.RemoveSheetAt(idx)
            'wb.CreateSheet(_sheet_nm)
            'wb.SetSheetOrder(_sheet_nm, idx)
            'wb.SetActiveSheet(idx)

            Dim sheet = wb.GetSheet(_sheet_nm)
            For Each row As HSSFRow In sheet
                row.RemoveAllCells()
            Next

            'reopen the stream and write it out
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try



    End Sub

    Private Shared Sub rename_worksheet()

        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
            End Using

            'Does the current sheet exist?
            Dim idx = wb.GetSheetIndex(_sheet_nm)
            If idx = -1 Then
                Throw New Exception("Sheet [" + _sheet_nm + "] does not exist")
            End If

            'Does the target sheet already exist?
            Dim idx1 = wb.GetSheetIndex(_new_sheet_nm)
            If idx1 <> -1 Then
                Throw New Exception("Sheet [" + _new_sheet_nm + "] already exists. You cannot have two worksheets with the same name.")
            End If

            'rename it 
            wb.SetSheetName(wb.GetSheetIndex(_sheet_nm), _new_sheet_nm)

            'reopen the stream and write it out
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try



    End Sub

    Private Shared Sub add_worksheet()

        Try
            Dim wb As HSSFWorkbook
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb = New HSSFWorkbook(fstream)
                'after the new hssfworkbook event, it closes the file stream
            End Using

            'add sheet
            Dim idx = wb.GetSheetIndex(_sheet_nm)
            If idx <> -1 Then
                Throw New Exception("Sheet [" + _sheet_nm + "] already exists")
            End If

            wb.CreateSheet(_sheet_nm)

            'reopen the stream and write it out
            Using fstream As New FileStream(_file_path, FileMode.Open)
                wb.Write(fstream)
            End Using
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try


    End Sub

    Private Shared Sub export_excel_03_data()
        Try

            Dim wb As HSSFWorkbook

            Dim column_meta As New List(Of export_column)

            'if the file is already there, read in the stream
            If File.Exists(_file_path) Then
                Using fstream As New FileStream(_file_path, FileMode.Open)
                    wb = New HSSFWorkbook(fstream)
                    'after the new hssfworkbook event, it closes the file stream
                End Using
            Else
                'create the workbook and add a sheet
                wb = New HSSFWorkbook()
                wb.CreateSheet(_sheet_nm)
            End If

            'set the data sheet
            Dim data_sheet As HSSFSheet = wb.GetSheet(_sheet_nm)

            'XLS does not support table styles

            Dim fFam As New System.Drawing.FontFamily("Calibri")
            Dim wb_font As New System.Drawing.Font(fFam, 11)

            Dim buffer_for_auto_filter As Boolean = False
            If _include_autofilter Then
                buffer_for_auto_filter = True
            End If

            'Write data

            Dim _total_columns As Integer

            'NPOI starts at base 0 for columns and rows
            Dim _row_index As Integer = 0
            Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
                cn.Open()

                Using cmd As New SqlClient.SqlCommand(_qry, cn)

                    Using reader = cmd.ExecuteReader

                        _total_columns = reader.FieldCount

                        Dim font = wb.CreateFont()
                        font.Boldweight = NPOI.SS.UserModel.FontBoldWeight.Bold
                        font.FontName = "Calibri"
                        font.FontHeightInPoints = 11

                        '---------------------------------------------------------------------------------------------------------------
                        'add headers

                        If _include_headers Then

                            Dim r As HSSFRow = data_sheet.CreateRow(_row_index)

                            For _col_index = 0 To _total_columns - 1

                                Dim col_nm As String = reader.GetName(_col_index)

                                Dim col As New export_column With {.ord_pos = _col_index.ToString, .col_width = i_get_cell_val_width(wb_font, col_nm, buffer_for_auto_filter), .col_nm = col_nm}
                                column_meta.Add(col)

                                '_export_col_widths.Add(col_nm, i_get_cell_val_width(wb_font, col_nm, buffer_for_auto_filter))

                                If _bold_headers Then
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

                            If _row_index >= 65536 Then
                                Throw New Exception("Excel XLS documents can only contain a maximum 65,536 rows per worksheet.")
                            End If

                            'add the row
                            Dim r As HSSFRow = data_sheet.CreateRow(_row_index)

                            'add the fields
                            For _col_index = 0 To _total_columns - 1

                                'for first row, add to dictionary
                                If Not _include_headers And _row_index = 0 Then
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

            If _include_autofilter Then
                data_sheet.SetAutoFilter(New NPOI.SS.Util.CellRangeAddress(0, _row_index - 1, 0, _total_columns - 1))
            End If

            'this way is very slow on large data sets
            'If _autofit_columns Then
            '    For i = 0 To _total_columns - 1
            '        data_sheet.AutoSizeColumn(i)
            '    Next
            'End If


            If _autofit_columns Then
                Dim j As Integer = 0
                For Each col In column_meta

                    'the units seem very jacked up for how they do this
                    '250 seems to work...go figure
                    Dim final_val = col.col_width * 250


                    data_sheet.SetColumnWidth(j, CInt(final_val))
                    j += 1
                Next
            End If

            Dim xls_file As New FileStream(_file_path, FileMode.OpenOrCreate)

            wb.Write(xls_file)
            xls_file.Close()
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try


    End Sub

    Private Shared Function i_get_cell_val_width(font_nm As System.Drawing.Font, cell_val As String, using_auto_filter As Boolean) As Double

        Try
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
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
            Return Nothing
        End Try


    End Function

    Private Shared Sub write_cell(row As HSSFRow, format_type As String, reader As SqlDataReader, index As Integer, wb As HSSFWorkbook)

        Try
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
                        Dim cell As HSSFCell = row.CreateCell(index)

                        'Dim cellStyle As HSSFCellStyle = wb.CreateCellStyle()

                        'cell.CellStyle.s

                        'cell.SetCellValue(reader.GetDateTime(index))

                        'cellStyle.DataFormat = "mm/dd/yy"
                    Case Else
                        'write as string
                        row.CreateCell(index).SetCellValue(reader(index).ToString)
                End Select
            End If
        Catch ex As Exception
            _xl_err_msg = ex.Message.ToString
        End Try

    End Sub

End Class

