

'NOTE: This in the end does not work due to the OpenXml packaging class unable to handle more than 10MB in multiple threads
' There was a open xml version 2.6 that fixes this, but its not strong named signed.
' After attempting to strong sign it, it threw many compilation errors that could not get fixed.
' Using NPOI to do the stuff


'Imports DocumentFormat.OpenXml.Spreadsheet
'Imports DocumentFormat.OpenXml.Packaging
'Imports System.Windows.Forms
'Imports System.Drawing
'Friend Class cExcel07

'    '----------------------------------------------------------------------------------------
'    'Functions: 
'    '1: export data - Done
'    '2: Add Worksheet - done
'    '3: Rename Worksheet - done
'    '4: Clear Worksheet - done
'    '----------------------------------------------------------------------------------------
'    'In Dev:
'    '1: Import Data
'    '2: Delete Sheet

'    Private Shared _XLPackage As SpreadsheetDocument
'    Private Shared _wbPart As WorkbookPart
'    Private Shared _wb As Workbook
'    Private Shared _xl_err_msg As String = ""
'    Private Shared _worksheets As New Dictionary(Of String, String)
'    Private Shared _tables As New Dictionary(Of String, String)
'    Private Shared _sharedStrings As New Dictionary(Of String, String)
'    Private Shared _sharedStrings_max_index As Integer = 0
'    Private Shared _worksheet_max_id As Integer = 1
'    Private Shared _table_max_id As Integer = 1
'    Private Shared _bold_font_style_index As Integer
'    Private Shared _date_style_index As Integer = 1
'    'Private Shared _export_col_widths As New Dictionary(Of String, Double)
'    Private Shared _row_index As Integer = 1
'    Private Shared _col_cnt As Integer
'    Private Shared _XL_sheet_row_max As Integer = 1048576


'    'in testing, the use of shared strings does not really help with compression. it is faster to just not use it and write strings directly
'    Private Shared _use_shared_strings As Boolean = False

'    '-------------------------------------------------------------------
'    'user inputs

'    Private Shared _file_path As String = EDIS.get_edis_pval("file_path")
'    Private Shared _sheet_nm As String = EDIS.get_edis_pval("sheet_nm")
'    Private Shared _new_sheet_nm As String = EDIS.get_edis_pval("new_sheet_nm")
'    Private Shared _table_nm As String = EDIS.get_edis_pval("table_nm")
'    Private Shared _use_auto_filter As String = EDIS.get_edis_pval("use_autofilter")
'    Private Shared _bold_headers As String = EDIS.get_edis_pval("bold_headers")
'    Private Shared _auto_fit_columns As String = EDIS.get_edis_pval("autofit_cols")
'    Private Shared _include_headers As String = EDIS.get_edis_pval("include_headers")
'    'Private Shared _include_auto_filter As string
'    Private Shared _src_qry As String = EDIS.get_edis_pval("src_qry")
'    Private Shared _mssql_inst As String = EDIS._mssql_inst
'    Private Shared _table_style_nm As String = EDIS.get_edis_pval("table_style") ' "TableStyleMedium3"
'    Private Shared _output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")


'    Shared Sub main(ByRef err_msg As String)

'        Try
'            Dim sub_task As String = EDIS.get_edis_pval("sub_task")

'            log_edis_event("info", "Excel 07", "sub task " + sub_task)

'            Select Case sub_task.ToLower
'                Case "export_data"
'                    log_edis_event("info", "Excel 07", "going to export data proc")
'                    export_excel_07_data(err_msg)
'                Case "rename_worksheet"
'                    u_rename_worksheet()
'                Case "clear_worksheet"
'                    u_clear_worksheet()
'                Case "add_worksheet"
'                    u_add_worksheet()
'                Case "get_sheet_list"
'                    u_get_sheet_list()
'                Case Else
'                    Throw New Exception("Task [" + sub_task + "] does not exist in excel 07 toolset")
'            End Select
'            'err_msg = _xl_err_msg
'        Catch ex As Exception
'            err_msg = ex.Message.ToString()
'        End Try

'    End Sub

'    Private Class export_column
'        Property ord_pos As String
'        Property col_width As Double
'        Property col_nm As String
'    End Class


'    Private Shared Sub export_excel_07_data(ByRef err_msg As String)

'        Try

'            'Synopsis
'            ' This export overwrites the existing tab
'            ' It will preserve the primary font and size from Excel, but all other stuff gets blown away
'            ' Future enhancemment: Have it read the existing sheet on a reader up to the sheet data, then overwrite...
'            '   link: https://blogs.msdn.microsoft.com/brian_jones/2010/06/22/writing-large-excel-files-with-the-open-xml-sdk/
'            '       this could be tricky though as after sheet data, there might be other junk
'            '   plus we will have to detect things like existing tables and what to do

'            '_bold_headers = True
'            '_auto_fit_columns = True

'            log_edis_event("info", "Excel 07", "export data started")

'            Dim buffer_for_auto_filter As Boolean = False
'            If _use_auto_filter Or _table_nm <> "" Then
'                buffer_for_auto_filter = True
'            End If

'            'counter for rows
'            Dim row_counter As Integer = 1

'            Dim sheet_range_referene As String = ""

'            If IO.File.Exists(_file_path) Then
'                i_open_workbook()
'            Else
'                i_create_workbook()
'                i_open_workbook()
'            End If

'            log_edis_event("info", "Excel 07", "workbook loaded")

'            '-------------------------------------------------------
'            'Check if the table name exists on another worksheet...if so, throw error

'            'Still getting errors if sheets exist on other worksheets

'            If _tables.ContainsKey(_table_nm) Then
'                For Each wsPart In _XLPackage.WorkbookPart.WorksheetParts
'                    Dim curr_sheet_nm As String = get_sheet_nm_from_wspart(wsPart)
'                    'dont evaluate if the sheet is the one we are writing to, since we will be replacing anyways
'                    If curr_sheet_nm <> _sheet_nm Then
'                        For Each itm In wsPart.TableDefinitionParts
'                            If itm.Table.Name = _table_nm Then
'                                Dim table_error_msg As String =
'                                    "Table [" + _table_nm + "] already exists on worksheet [" + curr_sheet_nm + "]. Table names must be unique across the workbook."

'                                err_msg = table_error_msg
'                                Exit Sub

'                            End If
'                        Next
'                    End If

'                Next
'            End If

'            log_edis_event("info", "Excel 07", "tables check complete")

'            Dim column_meta As New List(Of export_column)

'            Using _XLPackage

'                'default font for tracking width
'                Dim fFam As New System.Drawing.FontFamily("Calibri")
'                Dim font As New System.Drawing.Font(fFam, 11)


'                '--------------------------------------------------------------------------------------------------------------------------------
'                'create the style sheet
'                i_create_Style_Sheet()

'                'add worksheet part
'                Dim wsPart As WorksheetPart = _wbPart.AddNewPart(Of WorksheetPart)()

'                'This gets added when we create the openxml writer
'                'wsPart.Worksheet = New Worksheet(New SheetData())

'                log_edis_event("info", "Excel 07", "style sheet loaded")

'                Dim writer As DocumentFormat.OpenXml.OpenXmlWriter = DocumentFormat.OpenXml.OpenXmlWriter.Create(wsPart)

'                Using cn As New SqlClient.SqlConnection("Server= " + _mssql_inst + "; Integrated Security=  True")
'                    cn.Open()
'                    Dim cmd As New SqlClient.SqlCommand
'                    cmd.Connection = cn
'                    cmd.CommandText = _src_qry
'                    cmd.CommandType = CommandType.Text

'                    Dim oxa As List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'                    'create the worksheet and sheetdata for the Sheet
'                    writer.WriteStartElement(New Worksheet())
'                    writer.WriteStartElement(New SheetData())


'                    _row_index = 1

'                    Using reader = cmd.ExecuteReader

'                        _col_cnt = reader.FieldCount

'                        If _include_headers Then
'                            '----------------------------------------------------------------------------
'                            'Add Headers

'                            oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'                            'add row index : NOT required...leaving out for compression purposes
'                            ' oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, counter.ToString))
'                            'writer.WriteStartElement(New Row(), oxa)
'                            writer.WriteStartElement(New Row())

'                            'Add field names

'                            For i = 0 To _col_cnt - 1

'                                Dim cell_val As String = reader.GetName(i)
'                                Dim cell_ref As String = i_get_XL_Col_Letter_From_Number(i + 1) + row_counter.ToString

'                                'add column width to dictionary

'                                Dim col As New export_column With {.ord_pos = i, .col_width = i_get_cell_val_width(font, cell_val, buffer_for_auto_filter), .col_nm = cell_val}
'                                column_meta.Add(col)
'                                '_export_col_widths.Add(cell_val, i_get_cell_val_width(font, cell_val, buffer_for_auto_filter))
'                                ' _export_col_widths.Add(i.ToString, i_get_cell_val_width(font, cell_val, buffer_for_auto_filter))

'                                i_write_cell_value(writer, cell_val, cell_ref, "varchar")

'                            Next

'                            'close header row
'                            writer.WriteEndElement()

'                            _row_index += 1

'                            log_edis_event("info", "Excel 07", "headers added")

'                        End If



'                        '--------------------------------------------------------------------------------------------------------------------------
'                        'Add Data

'                        While reader.Read

'                            oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'                            'add row index : NOT required...leaving out for compression purposes
'                            'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, counter.ToString))
'                            'writer.WriteStartElement(New Row(), oxa)
'                            writer.WriteStartElement(New Row())

'                            'add field values
'                            For i = 0 To reader.FieldCount - 1

'                                'if we did not include headers, then add the first row to the export cols
'                                If Not _include_headers And _row_index = 1 Then

'                                    Dim col As New export_column With {.ord_pos = i, .col_width = i_get_cell_val_width(font, reader(i).ToString, buffer_for_auto_filter), .col_nm = "COL_" + i.ToString}
'                                    column_meta.Add(col)
'                                    '_export_col_widths.Add(i.ToString, i_get_cell_val_width(font, reader(i).ToString, buffer_for_auto_filter))
'                                End If

'                                Dim cell_val As String = reader(i).ToString
'                                Dim cell_ref As String = i_get_XL_Col_Letter_From_Number(i + 1) + row_counter.ToString
'                                Dim data_type As String = reader.GetDataTypeName(i).ToString.ToLower

'                                'evaluate the width
'                                If row_counter <= 100 Then
'                                    'Dim curr_catalog_row_width As Double = _export_col_widths.Item(reader.GetName(i))
'                                    'Dim curr_catalog_row_width As Double = _export_col_widths.Item(i.ToString)

'                                    Dim curr_catalog_row_width As Double = column_meta.Item(i).col_width

'                                    Dim this_row_width As Double = i_get_cell_val_width(font, cell_val, buffer_for_auto_filter)

'                                    If this_row_width > curr_catalog_row_width Then

'                                        column_meta.Item(i).col_width = this_row_width

'                                        '_export_col_widths.Item(i.ToString) = this_row_width
'                                        '_export_col_widths.Item(reader.GetName(i)) = this_row_width
'                                    End If

'                                End If


'                                i_write_cell_value(writer, cell_val, cell_ref, data_type)

'                            Next

'                            'close row
'                            writer.WriteEndElement()

'                            _row_index += 1

'                        End While

'                        'back off counter 1 since we finished the looop
'                        _row_index -= 1

'                        log_edis_event("info", "Excel 07", "data loaded")

'                        'check that we have not exceeded excels limit on rows
'                        If _row_index >= (_XL_sheet_row_max + 1) Then
'                            err_msg = "Excel Error: Data set exceeds maximum Excel row limit of [" + _XL_sheet_row_max + "]."

'                            Exit Sub
'                        End If

'                        'close sheetdata
'                        writer.WriteEndElement()

'                        'set the sheet range reference:
'                        sheet_range_referene = "A1:" + i_get_XL_Col_Letter_From_Number(_col_cnt) + _row_index.ToString

'                        If _use_auto_filter And _table_nm = "" Then
'                            writer.WriteElement(New AutoFilter() With {.Reference = sheet_range_referene})
'                        End If


'                        'close worksheet
'                        writer.WriteEndElement()

'                    End Using

'                End Using

'                writer.Close()

'                log_edis_event("info", "Excel 07", "writer closed")

'                '-----------------------------------------------------------------------------------------------------
'                'Add Columns if Auto-fitting 

'                If _auto_fit_columns Then
'                    Dim cCnt As Integer = 1
'                    Dim cols As New Columns
'                    For Each col As export_column In column_meta
'                        Dim c As New Column() With {.BestFit = True, .CustomWidth = True, .Width = col.col_width.ToString, .Max = cCnt.ToString, .Min = cCnt.ToString}
'                        cols.Append(c)
'                        cCnt += 1

'                    Next
'                    Dim sheetData = wsPart.Worksheet.GetFirstChild(Of SheetData)

'                    'Note: Columns must go before the sheet data, or excel cant read it
'                    wsPart.Worksheet.InsertBefore(cols, sheetData)

'                    log_edis_event("info", "Excel 07", "autofit cols applied")
'                End If

'                '---------------------------------------------------------------------------------------------------------------------------------------
'                'add table if specified
'                If _table_nm <> "" Then

'                    ' Dim tbID As String = "rId" + Left(Guid.NewGuid().ToString, 8)
'                    'Dim tbID As String = "rId" + _tables.Count.ToString

'                    Dim tblDefPart As TableDefinitionPart = wsPart.AddNewPart(Of TableDefinitionPart) '(tbID)

'                    Dim tbID As String = wsPart.GetIdOfPart(tblDefPart)

'                    Dim tb As New Table() With {.Id = _tables.Count.ToString + 1, .Name = _table_nm, .DisplayName = _table_nm, .Reference = sheet_range_referene, .TotalsRowShown = False}

'                    Dim af As New AutoFilter With {.Reference = sheet_range_referene}

'                    'add tablecolumns
'                    Dim tCols As New TableColumns With {.Count = column_meta.Count}

'                    Dim tcolCnt As Integer = 1
'                    For Each col As export_column In column_meta
'                        Dim tc As New TableColumn() With {.Id = tcolCnt.ToString, .Name = col.col_nm.ToString}
'                        tCols.Append(tc)
'                        tcolCnt += 1
'                    Next

'                    'add table style
'                    Dim tbStyleInfo As New TableStyleInfo() With {.Name = _table_style_nm, .ShowFirstColumn = False, .ShowRowStripes = True, .ShowColumnStripes = False}

'                    'add style, autofilter, and columns to the table

'                    tb.Append(af)
'                    tb.Append(tCols)
'                    tb.Append(tbStyleInfo)

'                    'relate to teh table def part
'                    tblDefPart.Table = tb



'                    'append to the worksheet
'                    Dim tparts_cnt = wsPart.Worksheet.Elements(Of TableParts).Count

'                    If tparts_cnt = 0 Then
'                        tparts_cnt = 1
'                    End If

'                    Dim tableParts As New TableParts() With {.Count = tparts_cnt.ToString}
'                    Dim tblPart As New TablePart() With {.Id = tbID}
'                    tableParts.Append(tblPart)

'                    wsPart.Worksheet.Append(tableParts)

'                    log_edis_event("info", "Excel 07", "table added")

'                End If

'                '------------------------------------------------------------------------------------------------------
'                'Add Shared Strings Catalog

'                'need to update this to detect if an existing sharedstring part exists...and if so, pull in that catalog, drop existing sharestrings, and write new one

'                If _sharedStrings.Count > 0 And _use_shared_strings Then

'                    'drop/replace existing shared strings
'                    If Not _wbPart.GetPartsOfType(Of SharedStringTablePart) Is Nothing Then

'                        _wbPart.DeletePart(_wbPart.SharedStringTablePart)
'                    End If

'                    Dim sharedStrings = _wbPart.AddNewPart(Of SharedStringTablePart)

'                    Using ssWriter As DocumentFormat.OpenXml.OpenXmlWriter = DocumentFormat.OpenXml.OpenXmlWriter.Create(sharedStrings)

'                        ssWriter.WriteStartElement(New SharedStringTable)
'                        For Each itm In _sharedStrings
'                            ssWriter.WriteStartElement(New SharedStringItem())
'                            ssWriter.WriteElement(New Text(itm.Key))
'                            ssWriter.WriteEndElement()
'                        Next

'                        ssWriter.WriteEndElement()

'                    End Using

'                End If

'                'Assign to the sheet

'                'Check if sheet exists
'                If _worksheets.ContainsKey(_sheet_nm) Then

'                    'get the existing sheet
'                    Dim tgt_sheet As Sheet = _wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = _sheet_nm.ToLower).FirstOrDefault()

'                    'grab the original ws part (aka xml file) because we are going to drop it
'                    Dim orig_ws_part As WorksheetPart = CType(_wbPart.GetPartById(tgt_sheet.Id), WorksheetPart)

'                    'set the target sheet to the new wb part
'                    tgt_sheet.Id.Value = _wbPart.GetIdOfPart(wsPart)

'                    'drop original
'                    _wbPart.DeletePart(orig_ws_part)
'                Else
'                    'add the sheet
'                    Dim sheets As Sheets = _wb.Elements(Of Sheets).FirstOrDefault
'                    Dim sh As Sheet = New Sheet() With {.Id = _wbPart.GetIdOfPart(wsPart), .SheetId = (_worksheets.Count + 1).ToString, .Name = _sheet_nm}
'                    sheets.Append(sh)
'                End If

'                log_edis_event("info", "Excel 07", "done")

'                'back off the row index 1 since we auto incriment after each row
'                EDIS.log_rows_tsfr(_row_index - 1)

'            End Using
'        Catch ex As Exception
'            err_msg = ex.Message.ToString()

'        End Try

'    End Sub

'    Private Shared Sub u_get_sheet_list()
'        Try
'            log_edis_event("info", "Excel 07", "Sheet list task started")

'            i_open_workbook()

'            log_edis_event("info", "Excel 07", "workbook opened")

'            Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
'                cn.Open()
'                log_edis_event("info", "Excel 07", "connection to SQL Server [" + _mssql_inst + "] opened")
'                For Each itm In _worksheets
'                    Dim sql As String = "INSERT [" + _output_tbl_nm + "] VALUES (@sheet_nm)"
'                    Using cmd As New SqlClient.SqlCommand(sql, cn)
'                        cmd.Parameters.AddWithValue("@sheet_nm", itm.Key.ToString)
'                        cmd.ExecuteNonQuery()
'                        log_edis_event("info", "Excel 07", "Worksheet [" + itm.Key.ToString + " loaded to table [" + _output_tbl_nm + "]")
'                    End Using
'                Next
'            End Using

'            log_edis_event("info", "Excel 07", "Sheet List Task ended...no errors")

'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'            log_edis_event("error", "Excel 07", "Sheet List Task Error: " + _xl_err_msg)
'        End Try
'    End Sub

'    Private Shared Sub u_rename_worksheet()

'        Try
'            i_open_workbook()

'            If Not _worksheets.ContainsKey(_sheet_nm) Then
'                _xl_err_msg = "Worksheet rename failure: Worksheet [" + _sheet_nm + "] does not exist."
'                Exit Sub
'            End If

'            'does the new sheet already exist?
'            If _worksheets.ContainsKey(_new_sheet_nm) Then
'                _xl_err_msg = "Worksheet rename failure: Worksheet [" + _new_sheet_nm + "] already exists. You cannot have 2 worksheets with the same name"
'                Exit Sub
'            End If

'            Dim ws As Sheet = _wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = _sheet_nm.ToLower).FirstOrDefault()

'            ws.Name = _new_sheet_nm

'            i_save_and_close_workbook()

'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try

'    End Sub

'    Private Shared Sub u_clear_worksheet()

'        Try
'            i_open_workbook()

'            If Not _worksheets.ContainsKey(_sheet_nm) Then
'                _xl_err_msg = "Sheet [" + _sheet_nm + "] does Not exist"
'                Exit Sub
'            End If

'            Dim ws As Sheet = _XLPackage.WorkbookPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = _sheet_nm.ToLower).FirstOrDefault()

'            Dim wsPart As WorksheetPart = CType(_wbPart.GetPartById(ws.Id), WorksheetPart)

'            Dim sheetData As SheetData = wsPart.Worksheet.Elements(Of SheetData)().First()

'            sheetData.RemoveAllChildren()

'            log_edis_event("info", "clear worksheet", "data clear")

'            'if a table exists, remove as well
'            'doesnt work
'            For Each tblDef As TableDefinitionPart In wsPart.TableDefinitionParts


'                wsPart.DeletePart(tblDef)
'                log_edis_event("info", "clear worksheet", "table [" + tblDef.Table.Name.ToString + "] removed")
'            Next

'            'log_edis_event("info", "clear worksheet", "tables removed")

'            'wsPart.Worksheet.Save()

'            i_save_and_close_workbook()

'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try

'    End Sub

'    Private Shared Sub u_add_worksheet()

'        Try

'            i_open_workbook()

'            If _worksheets.ContainsKey(_sheet_nm) Then
'                _xl_err_msg = "Add Worksheet failure: Worksheet [" + _sheet_nm + "] already exists"
'                Exit Sub
'            End If

'            Dim wspart As WorksheetPart = _wbPart.AddNewPart(Of WorksheetPart)
'            wspart.Worksheet = New Worksheet(New SheetData())

'            Dim sheets As Sheets = _wbPart.Workbook.GetFirstChild(Of Sheets)
'            Dim relId As String = _wbPart.GetIdOfPart(wspart)

'            Dim sheet_id As UInteger = 1
'            If sheets.Elements(Of Sheet).Count > 0 Then
'                sheet_id = sheets.Elements(Of Sheet).Select(Function(s) s.SheetId.Value).Max + 1
'            End If

'            Dim Sheet As New Sheet
'            With Sheet
'                .Id = relId
'                .SheetId = sheet_id
'                .Name = _sheet_nm
'            End With

'            sheets.Append(Sheet)

'            i_save_and_close_workbook()

'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try
'    End Sub

'    '=========================================================================================================================================
'    '=========================================================================================================================================
'    '=========================================================================================================================================
'    '=========================================================================================================================================
'    'Internal Functions

'    Private Shared Sub i_save_and_close_workbook()
'        Try
'            _wb.Save()
'            _XLPackage.Close()
'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try
'    End Sub

'    Private Shared Function i_open_workbook() As SpreadsheetDocument

'        Try
'            'testing
'            'file_path = "C:\temp\Book1.xlsx"

'            _XLPackage = SpreadsheetDocument.Open(_file_path, True)

'            _wbPart = _XLPackage.WorkbookPart
'            _wb = _wbPart.Workbook

'            '---------------------------------------------------------------------------------------------------
'            'get inventory

'            'worksheets
'            i_get_worksheets()

'            'tables
'            i_get_tables()

'            'shared strings
'            Dim stringTable = _XLPackage.WorkbookPart.GetPartsOfType(Of SharedStringTablePart).FirstOrDefault()

'            If Not stringTable Is Nothing Then
'                Dim ssArray = stringTable.SharedStringTable.Elements(Of SharedStringItem)().ToArray()

'                For Each item In ssArray
'                    _sharedStrings.Add(item.InnerText.ToString, _sharedStrings.Count)
'                Next
'            End If

'            Return _XLPackage

'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'            Return Nothing
'        End Try

'    End Function

'    Private Shared Sub i_get_worksheets()
'        Try
'            For Each sh As Sheet In _XLPackage.WorkbookPart.Workbook.Sheets
'                _worksheets.Add(sh.Name, sh.SheetId)
'                If CInt(sh.SheetId.ToString) > _worksheet_max_id Then _worksheet_max_id = CInt(sh.SheetId.ToString)
'            Next
'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try
'    End Sub

'    Private Shared Function get_sheet_nm_from_wspart(wspart As WorksheetPart) As String

'        Dim relId As String = _XLPackage.WorkbookPart.GetIdOfPart(wspart)
'        For Each sh As Sheet In _XLPackage.WorkbookPart.Workbook.Sheets
'            If sh.Id.Value = relId Then
'                Return sh.Name
'            End If
'        Next

'    End Function

'    Private Shared Sub i_get_tables()
'        Try
'            For Each ws As WorksheetPart In _XLPackage.WorkbookPart.WorksheetParts
'                For Each tblDef As TableDefinitionPart In ws.TableDefinitionParts
'                    _tables.Add(tblDef.Table.Name, tblDef.Table.Id)
'                    If CInt(tblDef.Table.Id.ToString) > _table_max_id Then _table_max_id = CInt(tblDef.Table.Id.ToString)
'                Next
'            Next
'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try
'    End Sub

'    Private Shared Sub i_create_workbook()

'        Try
'            If IO.File.Exists(_file_path) Then
'                Throw New Exception("File [" + _file_path + "] already exists")
'            End If

'            Dim wb_type As DocumentFormat.OpenXml.SpreadsheetDocumentType

'            Select Case Right(_file_path, 4).ToLower
'                Case "xlsx"
'                    wb_type = DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook
'                Case "xlsm"
'                    wb_type = DocumentFormat.OpenXml.SpreadsheetDocumentType.MacroEnabledWorkbook
'            End Select

'            _XLPackage = SpreadsheetDocument.Create(_file_path, wb_type)

'            'add WB xml file
'            Dim wbPart As WorkbookPart = _XLPackage.AddWorkbookPart

'            'append workbook node to workbookpart xml file
'            wbPart.Workbook = New Workbook

'            'add worksheet xml file
'            Dim wsPart As WorksheetPart = wbPart.AddNewPart(Of WorksheetPart)()

'            'append worksheet/sheetdata nodes to worksheetpart xml file
'            wsPart.Worksheet = New Worksheet(New SheetData())

'            'add sheets collection
'            Dim sheets As Sheets = _XLPackage.WorkbookPart.Workbook.AppendChild(Of Sheets)(New Sheets())

'            'add the sheet
'            Dim ws As Sheet = New Sheet

'            'relate it to the worksheetpart
'            ws.Id = _XLPackage.WorkbookPart.GetIdOfPart(wsPart)

'            'assign the id and name
'            ws.SheetId = 1
'            ws.Name = _sheet_nm

'            'append the single sheet to the sheets collection
'            sheets.Append(ws)

'            'save and close
'            wbPart.Workbook.Save()
'            _XLPackage.Close()

'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'        End Try

'    End Sub

'    Private Shared Function i_get_cell_val_width(font_nm As System.Drawing.Font, cell_val As String, using_auto_filter As String) As Double

'        Try
'            Dim autoFilter_icon_buffer
'            If using_auto_filter Then
'                autoFilter_icon_buffer = 2.5D
'            Else
'                autoFilter_icon_buffer = 0D
'            End If

'            Dim text_size As Size = TextRenderer.MeasureText(cell_val, font_nm)
'            Dim width As Double = CDbl(((text_size.Width / CDbl(7)) * 256) - (128 / 7)) / 256
'            width = CDbl(Decimal.Round(CDec(width) + 0.2D + autoFilter_icon_buffer, 2))

'            'cannot exceed 255...checking at 254 for rounding
'            If width > 254 Then
'                width = 255
'            End If

'            Return width
'        Catch ex As Exception
'            _xl_err_msg = ex.Message.ToString
'            Return Nothing
'        End Try


'    End Function

'    Private Shared Sub i_write_cell_value(writer As DocumentFormat.OpenXml.OpenXmlWriter, cell_val As String, cellRef As String, data_type As String)

'        Dim oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'        'set the reference attribute for the cell : Not required...we are leaving out for compression purposes
'        'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, cellRef))

'        If _bold_headers And _row_index = 1 Then
'            oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("s", Nothing, _bold_font_style_index.ToString))
'        End If

'        Select Case data_type
'            Case "int", "float", "smallint", "date", "tinyint", "bigint", "money", "numeric", "decimal"
'                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "n"))



'                If data_type = "date" And Trim(cell_val) <> "" Then

'                    'style for short date
'                    oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("s", Nothing, _date_style_index.ToString))

'                    'convert the date to excel number
'                    Dim dt As Date = CDate(cell_val)
'                    Dim dtInt = CInt((dt.[Date] - New DateTime(1900, 1, 1)).TotalDays) + 2

'                    cell_val = dtInt.ToString

'                End If

'                'write the cell
'                writer.WriteStartElement(New Cell(), oxa)
'                writer.WriteElement(New CellValue(cell_val))
'                writer.WriteEndElement()

'            Case "boolean"
'                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "b"))

'                'write the cell
'                writer.WriteStartElement(New Cell(), oxa)

'                If cell_val.ToLower = "true" Then
'                    cell_val = "1"
'                Else
'                    cell_val = "0"
'                End If

'                writer.WriteElement(New CellValue(cell_val))
'                writer.WriteEndElement()

'            Case Else

'                'incase we want to flip it off
'                If _use_shared_strings Then
'                    'shared string
'                    oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "s"))

'                    'add to shared string array if not exists
'                    If Not _sharedStrings.ContainsKey(cell_val) Then
'                        _sharedStrings.Add(cell_val, _sharedStrings.Count)
'                    End If

'                    writer.WriteStartElement(New Cell(), oxa)
'                    writer.WriteElement(New CellValue(_sharedStrings(cell_val).ToString()))
'                    writer.WriteEndElement()
'                Else
'                    'shared string
'                    oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "str"))

'                    writer.WriteStartElement(New Cell(), oxa)
'                    writer.WriteElement(New CellValue(cell_val))
'                    writer.WriteEndElement()
'                End If



'        End Select
'    End Sub

'    Private Shared Function i_get_XL_Col_Letter_From_Number(colIndex As Integer) As String
'        Dim div As Integer = colIndex
'        Dim colLetter As String = String.Empty
'        Dim modnum As Integer = 0

'        While div > 0
'            modnum = (div - 1) Mod 26
'            colLetter = Chr(65 + modnum) & colLetter
'            div = CInt((div - modnum) \ 26)
'        End While

'        Return colLetter
'    End Function

'    Private Shared Sub i_create_Style_Sheet()

'        Dim did_style_sheet_exist As Boolean

'        Dim stylePart = _wbPart.WorkbookStylesPart

'        If stylePart Is Nothing Then
'            did_style_sheet_exist = False
'            stylePart = _wbPart.AddNewPart(Of WorkbookStylesPart)()
'        Else
'            did_style_sheet_exist = True
'        End If

'        'if the style sheet already exists, we just need to add a new font with bold

'        If did_style_sheet_exist Then
'            Dim sSheet = stylePart.Stylesheet

'            'Get the default font (style 0)
'            Dim f = sSheet.Fonts.FirstOrDefault

'            'does the default go 
'            'get the properties
'            Dim fSize As String = f.Descendants(Of FontSize).FirstOrDefault.Val
'            Dim color_theme As String = f.Descendants(Of DocumentFormat.OpenXml.Spreadsheet.Color).FirstOrDefault.Theme
'            Dim color_rgb As String = f.Descendants(Of DocumentFormat.OpenXml.Spreadsheet.Color).FirstOrDefault.Rgb

'            Dim fName As String = f.Descendants(Of FontName).FirstOrDefault.Val

'            'append the new font with a bold
'            Dim fnew As New DocumentFormat.OpenXml.Spreadsheet.Font( 'Our bold headers ix 1
'                        New Bold(),
'                        New FontSize() With {.Val = fSize},
'                        New DocumentFormat.OpenXml.Spreadsheet.Color() With {.Theme = color_theme, .Rgb = color_rgb},
'                        New FontName() With {.Val = fName}
'                    )
'            sSheet.Fonts.Append(fnew)
'            'New Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},

'            _bold_font_style_index = (sSheet.Fonts.Elements(Of DocumentFormat.OpenXml.Spreadsheet.Font).Distinct.Count - 1).ToString

'            Dim cF As New CellFormat() With {.FontId = _bold_font_style_index, .FillId = 0, .BorderId = 0, .ApplyFont = True}

'            sSheet.CellFormats.Append(cF)

'            '------------------------------------------------------------------------------------------------------------------------
'            'append a datestyle format as well

'            Dim cfDate As New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0, .NumberFormatId = 14, .ApplyNumberFormat = True}

'            sSheet.CellFormats.Append(cfDate)

'            _date_style_index = (sSheet.CellFormats.Elements(Of CellFormat).Distinct.Count - 1).ToString

'        Else

'            _bold_font_style_index = 1
'            _date_style_index = 2

'            stylePart.Stylesheet = New Stylesheet(
'               New Fonts(
'                    New DocumentFormat.OpenXml.Spreadsheet.Font( 'default
'                        New FontSize() With {.Val = 11},
'                        New DocumentFormat.OpenXml.Spreadsheet.Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
'                        New FontName() With {.Val = "Calibri"}
'                    ),
'                    New DocumentFormat.OpenXml.Spreadsheet.Font( 'Our bold headers ix 1
'                        New Bold(),
'                        New FontSize() With {.Val = 11},
'                        New DocumentFormat.OpenXml.Spreadsheet.Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
'                        New FontName() With {.Val = "Calibri"}
'                    )
'                ),
'               New Fills(
'                    New Fill(New PatternFill() With {.PatternType = PatternValues.None}) 'default
'                ),
'               New Borders(
'                    New Border( 'default
'                        New LeftBorder(),
'                        New RightBorder(),
'                        New TopBorder(),
'                        New BottomBorder(),
'                        New DiagonalBorder()
'                    )
'                ),
'               New CellFormats(
'                    New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0}, ' (index 0: default)
'                    New CellFormat() With {.FontId = _bold_font_style_index, .FillId = 0, .BorderId = 0, .ApplyFont = True}, 'bold (index 1)
'                    New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0, .NumberFormatId = 14, .ApplyNumberFormat = True} 'Shortdate (index 2) 'for short dates in MM/dd/yyyy (numberFormatID 14 is shortdate)
'                )
'            )
'        End If

'    End Sub

'End Class
