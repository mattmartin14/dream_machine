Imports DocumentFormat.OpenXml.Packaging
Imports DocumentFormat.OpenXml.Spreadsheet
Imports DocumentFormat.OpenXml
Imports System.IO
Imports System.Windows.Forms
Imports System.Drawing

Friend Class cXL07

    '-------------------------------------------------------------
    'Get sheet list: Done
    'Add Sheet: Done
    'Rename Sheet: Done
    'clear sheet: done
    'export data: done

    Friend Shared Sub import_data(file_path As String, sheet_nm As String, include_headers As Boolean, tgt_db As String, tgt_schema As String,
                                 tgt_tbl As String, mssql_inst As String, load_on_ordinal As Boolean, ByVal crt_dest_tbl As Boolean,
                                  ByVal src_row_sample_cnt As Integer, ByVal import_all_as_text As Boolean, ByVal batch_size As Integer,
                                  ByVal header_rows_to_skip As Integer, ByVal include_row_id As Boolean, ByRef err_msg As String)

        Dim err_prefix As String = "XL07 Import Data Error: "

        Dim col_index As Integer = 0

        Dim row_index As Integer = 0

        Try

            Dim d1904_offset As Integer = 0

            Dim is_first_load_call As Boolean = True
            Dim src_cols As New List(Of cDataTypeParser.src_column_metadata)
            Dim tgt_col_list As New List(Of String)

            'Bug Fix...no longer using the src_row_sample_cnt, just set it to the batch size so they are in sync
            src_row_sample_cnt = batch_size


            Using xlPackage As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, False)

                log_edis_event("info", "opened workbook", "start")
                Dim wb_part As Workbook = xlPackage.WorkbookPart.Workbook
                If wb_part.WorkbookProperties IsNot Nothing Then
                    If wb_part.WorkbookProperties.Date1904 IsNot Nothing Then
                        If wb_part.WorkbookProperties.Date1904.Value = True Then
                            d1904_offset = 1462
                        End If
                    End If
                End If


                Dim ws As Sheet = xlPackage.WorkbookPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()

                'log_edis_event("info", "got worksheet instance", ws.Name.ToString)

                If ws Is Nothing Then

                    Dim sheet_list As String = String.Join(vbCrLf, get_sheet_list(xlPackage, err_msg))

                    err_msg = "Sheet [" + sheet_nm + "] does not exist in workbook [" + file_path + "]." + vbCrLf
                    err_msg += "The following sheets are available in the workbook: " + vbCrLf
                    err_msg += sheet_list

                    Exit Sub
                End If

                Dim wsPart As WorksheetPart = CType(xlPackage.WorkbookPart.GetPartById(ws.Id), WorksheetPart)

                'log_edis_event("info", "worksheetpart selected", "start")

                'Do not run this, it will effectively load all the data to memory
                'Dim sheetData As SheetData = wsPart.Worksheet.Elements(Of SheetData)().First()

                Dim stringTable = xlPackage.WorkbookPart.GetPartsOfType(Of SharedStringTablePart).FirstOrDefault()
                Dim sharedStringItemsArray As SharedStringItem()

                If stringTable IsNot Nothing Then
                    sharedStringItemsArray = stringTable.SharedStringTable.Elements(Of SharedStringItem)().ToArray()
                End If

                log_edis_event("info", "shared strings selected", "start")

                Dim cellFormats
                Dim numberFormats

                Dim stylePart = xlPackage.WorkbookPart.WorkbookStylesPart

                If stylePart IsNot Nothing Then
                    cellFormats = xlPackage.WorkbookPart.WorkbookStylesPart.Stylesheet.CellFormats
                    numberFormats = xlPackage.WorkbookPart.WorkbookStylesPart.Stylesheet.NumberingFormats
                End If

                Dim dt As New DataTable

                log_edis_event("info", "dattable initiated", "start")

                '--------------------------------------------------------------------------------------------------
                'Read data

                Dim batch_id As Integer = 0
                Dim batch_row_cnt As Integer = 0
                Dim col_cnt As Integer = 0

                log_edis_event("info", "Excel Import", String.Format("header rows to skip {0}", header_rows_to_skip))

                Dim first_row_col_list As New List(Of String)

                Using reader As OpenXmlReader = OpenXmlReader.Create(wsPart)
                    log_edis_event("info", "Excel Import", "Opened Reader")
                    While reader.Read()

                        If reader.ElementType = GetType(Row) Then

                            Dim is_row_empty As Boolean = True

                            row_index += 1

                            If (row_index >= 1000 AndAlso Not EDIS._is_pro_user) Then
                                err_msg = "Importing more than 1,000 rows Is a pro-only feature. Please visit www.sqletl.com to purchase an EDIS Pro license."
                                Exit Sub
                            End If

                            batch_id += 1

                            reader.ReadFirstChild()


                            Dim dRow As DataRow = dt.NewRow

                            col_index = 0
                            'Dim col_pos As Integer = 0

                            Do 'read each cell in the row

                                log_edis_event("info", "Excel Import", "Reading Cell")

                                If reader.ElementType = GetType(Cell) Then
                                    Dim c = DirectCast(reader.LoadCurrentElement(), Cell)

                                    'grab cell column reference to handle missing cells
                                    If c.CellReference IsNot Nothing Then
                                        If c.CellReference.HasValue Then
                                            col_index = get_col_from_cell_adrs(c.CellReference.Value.ToString)
                                        End If
                                    End If


                                    Dim cVal As String = get_cell_val(c, sharedStringItemsArray, cellFormats, numberFormats,
                                                                      import_all_as_text, d1904_offset, err_msg)
                                    If err_msg <> "" Then
                                        Exit Sub
                                    End If

                                    log_edis_event("Info", "Excel Import", String.Format("Cell Value {0}", cVal))

                                    'for the first row, load into a list
                                    'If row_index = 1 Then
                                    If row_index = (header_rows_to_skip + 1) Then
                                        log_edis_event("Info", "Excel Import", "adding cell to header list")
                                        first_row_col_list.Add(cVal)
                                    Else
                                        If Not String.IsNullOrWhiteSpace(cVal) Then

                                            If row_index <> 1 And col_index <= col_cnt - 1 Then
                                                dRow(col_index) = Trim(cVal)
                                                is_row_empty = False
                                            End If


                                        Else
                                            If row_index <> 1 And col_index <= col_cnt - 1 Then
                                                dRow(col_index) = DBNull.Value
                                            End If

                                        End If
                                    End If

                                    col_index += 1
                                End If
                            Loop While reader.ReadNextSibling()

                            log_edis_event("Info", "Excel Import", "Finished Reading Excel Row")

                            log_edis_event("Info", "Excel Import", String.Format("row index {0}, header rows to skip {1}", row_index, header_rows_to_skip))

                            '-----------------------------------------------------------------------------------------------------------
                            'for first row, add the columns
                            If row_index = (header_rows_to_skip + 1) Then

                                log_edis_event("Info", "Excel Import", "Processing Headers")
                                'If row_index = 1 Then
                                'Dim col_cnt As Integer = 0
                                For Each col In first_row_col_list
                                    Dim col_nm As String
                                    If include_headers Then

                                        'strip out carriage returns from the header columns
                                        Dim cVal As String = col.ToString
                                        cVal = Replace(cVal, vbCrLf, "")
                                        cVal = Replace(cVal, vbLf, "")
                                        cVal = Replace(cVal, vbCr, "")

                                        col_nm = cVal
                                    Else
                                        col_nm = "COL_" + col_cnt.ToString
                                    End If
                                    dt.Columns.Add(col_nm, GetType(String))
                                    col_cnt += 1
                                Next

                                If include_row_id Then
                                    dt.Columns.Add("EDIS_ROW_ID", GetType(Integer))
                                    col_cnt += 1
                                End If

                                'if the first row does not include headers, add the data row
                                If Not include_headers Then
                                    Dim col_pos_init As Integer = 0

                                    For Each col In first_row_col_list
                                        If Not String.IsNullOrWhiteSpace(col) Then
                                            dRow(col_pos_init) = col
                                        Else
                                            dRow(col_pos_init) = DBNull.Value
                                        End If
                                        col_pos_init += 1
                                    Next

                                    dt.Rows.Add(dRow)

                                End If
                            Else

                                If row_index <= header_rows_to_skip Then
                                    GoTo skip_row
                                End If

                                If include_row_id Then
                                    dRow("EDIS_ROW_ID") = row_index
                                End If

                                'past first row...read on 
                                'dont add empty rows
                                If Not is_row_empty Then
                                    log_edis_event("Info", "Excel Import", "adding data row")
                                    dt.Rows.Add(dRow)
                                End If
                            End If

                            batch_row_cnt += 1

                        End If

                        If batch_row_cnt >= batch_size Then

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


                            'Console.WriteLine("bulk loading batch {0}", batch_id)
                            cDT_Tasks.bulk_insert(mssql_inst, tgt_db, tgt_schema, tgt_tbl, tgt_col_list, dt, batch_size, load_on_ordinal, "", "", err_msg)
                            If err_msg <> "" Then Exit Sub
                            dt.Clear()
                            batch_id += 1
                            batch_row_cnt = 0
                        End If

skip_row:

                    End While

                End Using

                'Load last batch if we didnt
                If batch_row_cnt > 0 Then

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

                    'Console.WriteLine("bulk loading batch {0}", batch_id)
                    cDT_Tasks.bulk_insert(mssql_inst, tgt_db, tgt_schema, tgt_tbl, tgt_col_list, dt, batch_size, load_on_ordinal, "", "", err_msg)
                    If err_msg <> "" Then Exit Sub
                    dt.Clear()
                End If

            End Using



            'back off if include headers
            If include_headers Then row_index -= 1

            'back off skip rows
            row_index -= header_rows_to_skip

            EDIS.log_rows_tsfr(row_index)

            'Console.WriteLine("total rows imported {0}", row_index)

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString '+ ex.StackTrace.ToString()
            If show_call_stack Then
                err_msg += ex.StackTrace.ToString()
            End If
        End Try


    End Sub


    Private Shared Function get_col_from_cell_adrs(address As String) As Integer

        Dim ci As Integer = 0

        address = address.ToUpper

        Dim ix As Integer = 0
        While ix < address.Length AndAlso address(ix) >= "A"c
            ci = (ci * 26) + (CInt(Asc(address(ix))) - 64)
            ix += 1
        End While

        Return (ci - 1) 'offset 1 for base 0

    End Function

    Private Shared Function get_cell_val(c As Cell, stringArray As SharedStringItem(), cellformats As CellFormats, numberFormats As NumberingFormats,
                                         import_as_text As Boolean, d1904_offset As Integer, ByRef err_msg As String) As String

        Dim err_prefix As String = "XL07 Get Cell Value Error "
        Try

            Dim val As String
            Dim orig_val As String

            'initial value
            If c.CellValue IsNot Nothing Then
                val = c.CellValue.InnerText
                'Else
                '    Return Nothing
            End If

            orig_val = val

            ''IF THEY ARE READING AS TEXT, just grab value and finish
            'If import_as_text Then
            '    'evalute shared string before existing
            '    If c.DataType IsNot Nothing AndAlso c.DataType.Value = CellValues.SharedString Then
            '        Dim ssi As SharedStringItem = stringArray(Integer.Parse(c.CellValue.InnerText))
            '        val = ssi.InnerText
            '    End If

            '    Return val
            '    Exit Function

            'End If

            'If not importing as text, process as is below.....

            Dim styleIndex, cf
            If c.StyleIndex IsNot Nothing Then
                styleIndex = c.StyleIndex
                cf = CType(cellformats.ElementAt(styleIndex.ToString), CellFormat)

            End If
            'Dim cf = CType(cellformats.ElementAt(styleIndex), CellFormat)

            If c.DataType IsNot Nothing Then
                Select Case c.DataType.Value
                    Case CellValues.SharedString
                        Dim ssi As SharedStringItem = stringArray(Integer.Parse(c.CellValue.InnerText))
                        val = Trim(ssi.InnerText)
                   ' val = stringtable.SharedStringTable.ElementAt(Integer.Parse(c.InnerText)).InnerText
                    Case CellValues.Boolean
                        Select Case val
                            Case "0"
                                val = "FALSE"
                            Case Else
                                val = "TRUE"
                        End Select

                    Case CellValues.InlineString

                        If c.InnerText IsNot Nothing Then
                            val = c.InnerText.ToString()
                        End If


                    Case CellValues.Date
                        val = DateTime.FromOADate(Double.Parse(val) + d1904_offset).ToShortDateString()
                End Select
            ElseIf (Not cf Is Nothing) AndAlso (Not cf.NumberFormatId Is Nothing) Then

                Select Case cf.NumberFormatId.Value
                    'Dates:
                    '14: "m/d/yyyy"
                    '15: d-mmm-yy
                    '16: d-mmm
                    '17: mmm-d
                    '22: m/d/yy h:mm
                    '164: yyyy-mm-dd hh:mm:ss -- not a valid default format

                    Case 14, 15, 16, 17, 22

                        val = DateTime.FromOADate(Double.Parse(val) + d1904_offset).ToString
                        Dim dVal As DateTime = DateTime.Parse(val)
                        If dVal.Hour = 0 And dVal.Minute = 0 And dVal.Second = 0 And dVal.Millisecond = 0 Then
                            val = dVal.ToShortDateString()
                        End If

                End Select
            End If

            If String.IsNullOrWhiteSpace(val) Then
                Return Nothing
            Else
                Return val
            End If

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString() '+ ex.StackTrace.ToString

        End Try

    End Function


    Friend Shared Sub get_sheet_list(ByVal file_path As String, ByVal mssql_inst As String,
            ByVal output_tbl_nm As String, ByRef err_msg As String
        )
        Dim err_prefix As String = "Excel Get Sheet List Error "

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Using xlPackage As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)
                    'load all sheet names to list
                    For Each sh As Sheet In xlPackage.WorkbookPart.Workbook.Sheets
                        Using cmd As New SqlClient.SqlCommand("INSERT [" + output_tbl_nm + "] VALUES (@sheet_nm)", cn)
                            cmd.Parameters.AddWithValue("@sheet_nm", sh.Name.ToString)
                            cmd.ExecuteNonQuery()
                        End Using
                    Next
                End Using
            End Using

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub

    Private Shared Function get_sheet_list(ByVal xlPackage As SpreadsheetDocument, ByRef err_msg As String) As List(Of String)

        Dim err_prefix As String = "Excel Get Sheet List(I) Error "

        Dim sheet_list As New List(Of String)

        Try

            'load all sheet names to list
            For Each sh As Sheet In xlPackage.WorkbookPart.Workbook.Sheets
                sheet_list.Add(sh.Name.ToString())
            Next

            Return sheet_list

        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
            Return Nothing
        End Try

    End Function



    Friend Shared Sub clear_worksheet(ByVal file_path As String, ByVal sheet_nm As String, ByRef err_msg As String)

        Dim err_prefix As String = "Excel Clear Worksheet Error "

        Try
            Using xlPackage As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)

                Dim ws As Sheet = xlPackage.WorkbookPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()

                If ws Is Nothing Then
                    err_msg = err_prefix + "Worksheet [" + sheet_nm + "] does Not exist"
                    Exit Sub
                End If

                Dim wsPart As WorksheetPart = CType(xlPackage.WorkbookPart.GetPartById(ws.Id), WorksheetPart)

                Dim sheetData As SheetData = wsPart.Worksheet.Elements(Of SheetData)().First()

                sheetData.RemoveAllChildren()

                xlPackage.WorkbookPart.Workbook.Save()

            End Using
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try
    End Sub

    Friend Shared Sub add_worksheet(ByVal file_path As String, ByVal sheet_nm As String, ByVal err_msg As String)
        Dim err_prefix As String = "Excel Add Worksheet Error "
        Try
            Using xlPackage As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)

                'does the worksheet already exist
                For Each sh As Sheet In xlPackage.WorkbookPart.Workbook.Sheets
                    If sh.Name.ToString.ToLower = sheet_nm Then
                        err_msg = err_prefix + "Sheet [" + sheet_nm + "] already exists"
                        Exit Sub
                    End If
                Next

                Dim wspart As WorksheetPart = xlPackage.WorkbookPart.AddNewPart(Of WorksheetPart)
                wspart.Worksheet = New Worksheet(New SheetData())

                Dim sheets As Sheets = xlPackage.WorkbookPart.Workbook.GetFirstChild(Of Sheets)
                Dim relId As String = xlPackage.WorkbookPart.GetIdOfPart(wspart)

                Dim sheet_id As UInteger = 1
                If sheets.Elements(Of Sheet).Count > 0 Then
                    sheet_id = sheets.Elements(Of Sheet).Select(Function(s) s.SheetId.Value).Max + 1
                End If

                Dim Sheet As New Sheet
                With Sheet
                    .Id = relId
                    .SheetId = sheet_id
                    .Name = sheet_nm
                End With

                sheets.Append(Sheet)

                xlPackage.WorkbookPart.Workbook.Save()

            End Using
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try

    End Sub

    Friend Shared Sub rename_worksheet(file_path As String, sheet_nm As String, new_sheet_nm As String, ByRef err_msg As String)

        Dim err_prefix As String = "Excel Rename Worksheet Error "
        Try
            Using xlPackage As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)

                'load all sheet names to list
                Dim sheet_names As New List(Of String)
                For Each sh As Sheet In xlPackage.WorkbookPart.Workbook.Sheets
                    sheet_names.Add(sh.Name.ToString)
                Next

                'does the current sheet exist?
                If Not sheet_names.Contains(sheet_nm) Then
                    err_msg = err_prefix + "Workbook does Not contain worksheet [" + sheet_nm + "]"
                    Exit Sub
                End If

                'does the rename already exist?
                If sheet_names.Contains(new_sheet_nm) Then
                    err_msg = err_prefix + "Workbook already contains worksheet [" + new_sheet_nm + "]"
                    Exit Sub
                End If

                'rename the sheet
                Dim ws As Sheet = xlPackage.WorkbookPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()

                ws.Name = new_sheet_nm

                xlPackage.WorkbookPart.Workbook.Save()

            End Using
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString
        End Try
    End Sub

    Shared Sub export_data(ByVal file_path As String, ByVal mssql_inst As String, ByVal src_qry As String, ByVal sheet_nm As String,
                           ByVal include_headers As Boolean, ByVal bold_headers As Boolean,
                           ByVal autofit_cols As Boolean, ByVal apply_autofilter As Boolean, ByRef err_msg As String
        )
        Dim err_prefix As String = "Excel Data Export Error "
        Try

            Dim row_index As Integer = 0

            If Not File.Exists(file_path) Then
                create_workbook(file_path, sheet_nm, err_msg)
                If err_msg <> "" Then
                    Exit Sub
                End If
            End If

            Using xlPackage As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)

                Dim wspart As WorksheetPart
                Dim ws As Sheet = xlPackage.WorkbookPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()

                'If worksheet doesn't exist, add it
                If ws Is Nothing Then

                    'add the worksheet if it doesn't exist
                    wspart = xlPackage.WorkbookPart.AddNewPart(Of WorksheetPart)
                    wspart.Worksheet = New Worksheet(New SheetData())

                    Dim sheets As Sheets = xlPackage.WorkbookPart.Workbook.GetFirstChild(Of Sheets)
                    Dim relId As String = xlPackage.WorkbookPart.GetIdOfPart(wspart)

                    Dim sheet_id As UInteger = 1
                    If sheets.Elements(Of Sheet).Count > 0 Then
                        sheet_id = sheets.Elements(Of Sheet).Select(Function(s) s.SheetId.Value).Max + 1
                    End If

                    Dim Sheet As New Sheet
                    With Sheet
                        .Id = relId
                        .SheetId = sheet_id
                        .Name = sheet_nm
                    End With

                    sheets.Append(Sheet)

                    'set the sheet target
                    ws = xlPackage.WorkbookPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()


                Else
                    wspart = CType(xlPackage.WorkbookPart.GetPartById(ws.Id), WorksheetPart)
                End If


                Dim sheetData As SheetData = wspart.Worksheet.Elements(Of SheetData)().First()

                'clear and load
                sheetData.RemoveAllChildren()

                '------------------------------------------------------------------------------------------------
                'Add Data
                Dim cols As New List(Of export_col)

                Dim fFam As New System.Drawing.FontFamily("Calibri")
                Dim font As New System.Drawing.Font(fFam, 11)

                Dim has_data As Boolean = True

                Dim row_max As Integer = 1048576

                Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                    cn.Open()

                    Using cmd As New SqlClient.SqlCommand(src_qry, cn)
                        cmd.CommandTimeout = 0
                        Using reader = cmd.ExecuteReader()

                            'get headers
                            For i = 0 To reader.FieldCount - 1
                                Dim col As New export_col
                                col.col_nm = reader.GetName(i).ToString
                                Dim dt As String = reader.GetDataTypeName(i).ToString.ToLower
                                col.data_type = dt
                                Select Case dt
                                    Case "int", "float", "bigint", "tinyint", "smallint", "money", "decimal", "numeric"
                                        col.xl_dataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number
                                    Case "boolean"
                                        col.xl_dataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Boolean
                                    Case Else
                                        col.xl_dataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String
                                End Select
                                col.ord_pos = i
                                col.cell_width = get_cell_val_width(font, reader.GetName(i).ToString, apply_autofilter)

                                cols.Add(col)
                            Next

                            If include_headers Then

                                Dim boldIndex As Integer
                                If bold_headers Then
                                    boldIndex = add_bold_index(xlPackage)
                                End If

                                Dim row As New Row()
                                For Each col In cols
                                    Dim cell As New Cell
                                    cell.CellValue = New CellValue(col.col_nm)
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String
                                    If bold_headers Then
                                        cell.StyleIndex = boldIndex
                                    End If
                                    row.Append(cell)
                                Next


                                sheetData.Append(row)
                            End If

                            'load data
                            If reader.HasRows Then

                                While reader.Read()

                                    If (row_index >= 1000 AndAlso Not EDIS._is_pro_user) Then
                                        err_msg = "Exporting more than 1,000 rows Is a pro-only feature. Please visit www.sqletl.com to purchase an EDIS Pro license."
                                        Exit Sub
                                    End If

                                    If row_index >= row_max Then
                                        err_msg = err_prefix + "Excel (" + Right(file_path, 4) + " documents can only contain a maximum 1,048,576 rows per worksheet. Your query exceeds that amount."
                                        reader.Close()
                                        cn.Close()
                                        cmd.Dispose()
                                        Exit Sub
                                    End If
                                    Dim row As New Row()
                                    For Each col In cols
                                        Dim cell As New Cell
                                        cell.CellValue = New CellValue(reader(col.ord_pos).ToString)
                                        cell.DataType = col.xl_dataType
                                        row.Append(cell)
                                        If row_index <= 100 AndAlso autofit_cols Then
                                            Dim calc_width As Double = get_cell_val_width(font, reader(col.ord_pos).ToString, apply_autofilter)
                                            If calc_width > col.cell_width Then
                                                col.cell_width = calc_width
                                            End If
                                        End If
                                    Next
                                    sheetData.Append(row)
                                    row_index += 1



                                End While
                            Else
                                has_data = False
                                row_index = 1 'set to one, we will back it off later

                                'if no rows and no headers, place an empty cell in a1 so the workbook can open
                                If Not include_headers Then
                                    Dim row As New Row()
                                    Dim cell As New Cell
                                    cell.CellValue = New CellValue("")
                                    row.Append(cell)
                                    sheetData.Append(row)
                                End If
                            End If

                        End Using
                    End Using
                End Using



                'autofit columns?
                If autofit_cols And has_data Then
                    Dim cCnt As Integer = 1
                    Dim xLCols As New Columns
                    For Each col In cols
                        Dim c As New Column() With {.BestFit = True, .CustomWidth = True, .Width = col.cell_width.ToString, .Max = cCnt.ToString, .Min = cCnt.ToString}
                        xLCols.Append(c)
                        cCnt += 1
                    Next

                    Dim existing_cols = wspart.Worksheet.Descendants(Of Columns).FirstOrDefault()
                    If Not existing_cols Is Nothing Then
                        wspart.Worksheet.RemoveAllChildren(Of Columns)()
                    End If

                    'Note: Columns must go before the sheet data, or excel cant read it
                    wspart.Worksheet.InsertBefore(xLCols, sheetData)
                Else

                End If

                'Autofilter?
                If apply_autofilter And has_data Then
                    Dim sheet_range_referene As String = "A1:" + get_XL_Col_Letter_From_Number(cols.Count) + (row_index + 1).ToString

                    Dim af = wspart.Worksheet.Descendants(Of AutoFilter).FirstOrDefault()
                    If af Is Nothing Then
                        af = New AutoFilter With {.Reference = sheet_range_referene}
                        wspart.Worksheet.InsertAfter(af, sheetData)
                    Else
                        af.Reference = sheet_range_referene
                    End If

                    'Add the defined name so that the autofilter can sort
                    Dim first_col_letter As String = "A"
                    Dim last_col_letter As String = get_XL_Col_Letter_From_Number(cols.Count)
                    Dim first_row As Integer = 1

                    Dim def_nm_str As String = String.Format("'{0}'!${1}${2}:${3}${4}", ws.Name, first_col_letter, first_row, last_col_letter, row_index + 1)

                    Dim df As New DefinedName(def_nm_str) With {.Name = "_xlnm._FilterDatabase", .LocalSheetId = Int(ws.SheetId) - 1, .Hidden = True}

                    Dim def_names = xlPackage.WorkbookPart.Workbook.DefinedNames

                    If def_names Is Nothing Then
                        def_names = New DefinedNames()
                        xlPackage.WorkbookPart.Workbook.Append(def_names)
                    End If

                    def_names.Append(df)


                End If

                xlPackage.WorkbookPart.Workbook.Save()



            End Using

            'fix the workbook for mobile
            'fix_xl_wb_for_mobile(file_path, err_msg)

            EDIS.log_rows_tsfr(row_index - 1)
        Catch ex As Exception
            err_msg = err_prefix + ex.Message.ToString '+ " " + ex.StackTrace.ToString
        End Try


    End Sub

    'Private Shared Sub fix_xl_wb_for_mobile(ByVal file_path As String, ByRef err_msg As String)



    'NOTES:
    'After trying this, the file "opens" in iOS, but none of the cell contents render.
    ' it looks like to do this correct, I'd have to go through all XML files and make them the same as how Excel saves them
    '   ....even when using a template, you get the same problem....not worth the effort right now


    '    Dim err_msg_prefix As String = "Excel 07 Error: Updating WB rels"

    '    Try
    '        Const documentRelationshipType As String = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
    '        Const relationshipSchema As String = "http://schemas.openxmlformats.org/package/2006/relationships"
    '        Dim documentUri As String = Nothing
    '        Dim documentDirectory As String = Nothing
    '        Dim documentName As String = Nothing

    '        Dim relDocUri As Uri = Nothing

    '        Dim targetAttributeName As XName = Nothing
    '        Dim targetValue As String = Nothing

    '        '  Open the package

    '        Using xlPackage As System.IO.Packaging.Package = System.IO.Packaging.Package.Open(file_path, FileMode.Open, FileAccess.ReadWrite)
    '            ' Get the directory and filename of the main document part (e.g. /xl/workbook.xml).
    '            For Each relationship As System.IO.Packaging.PackageRelationship In xlPackage.GetRelationshipsByType(documentRelationshipType)
    '                documentUri = relationship.TargetUri.ToString()

    '                documentName = System.IO.Path.GetFileName(documentUri)
    '                documentDirectory = documentUri.Substring(0, documentUri.Length - documentName.Length)

    '                '  There should only be document part in the package, but break out anyway.
    '                Exit For
    '            Next

    '            ' Load the relationship document
    '            relDocUri = New Uri((Convert.ToString(documentDirectory & Convert.ToString("_rels/")) & documentName) + ".rels", UriKind.Relative)
    '            Dim relDoc As XDocument = XDocument.Load(xlPackage.GetPart(relDocUri).GetStream())

    '            ' Loop through all of the relationship nodes
    '            targetAttributeName = XName.[Get]("Target")
    '            For Each relNode As XElement In relDoc.Elements(XName.[Get]("Relationships", relationshipSchema)).Elements(XName.[Get]("Relationship", relationshipSchema))
    '                ' Edit the value of the Target attribute
    '                targetValue = relNode.Attribute(targetAttributeName).Value

    '                If targetValue.StartsWith(documentDirectory) Then
    '                    targetValue = targetValue.Substring(documentDirectory.Length)
    '                End If

    '                relNode.Attribute(targetAttributeName).Value = targetValue
    '            Next

    '            ' Save the document
    '            relDoc.Save(xlPackage.GetPart(relDocUri).GetStream())
    '        End Using


    '    Catch ex As Exception

    '    End Try

    'End Sub

    Private Shared Function get_XL_Col_Letter_From_Number(colIndex As Integer) As String
        Dim div As Integer = colIndex
        Dim colLetter As String = String.Empty
        Dim modnum As Integer = 0

        While div > 0
            modnum = (div - 1) Mod 26
            colLetter = Chr(65 + modnum) & colLetter
            div = CInt((div - modnum) \ 26)
        End While

        Return colLetter
    End Function


    Private Shared Sub create_workbook(file_path As String, sheet_nm As String, ByRef err_msg As String)

        'tried using template to fix compatibility on iphones...didnt work
        'Select Case LCase(Right(file_path, 4))
        '    Case "xlsx"
        '        System.IO.File.WriteAllBytes(file_path, My.Resources.Book1_xlsx)
        '    Case "xlsm"
        '        System.IO.File.WriteAllBytes(file_path, My.Resources.Book1_xls)
        'End Select

        'rename_worksheet(file_path, "template", sheet_nm, err_msg)

        'Exit Sub

        Dim wb_type As DocumentFormat.OpenXml.SpreadsheetDocumentType

        Select Case Right(file_path, 4).ToLower
            Case "xlsx"
                wb_type = DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook
            Case "xlsm"
                wb_type = DocumentFormat.OpenXml.SpreadsheetDocumentType.MacroEnabledWorkbook
        End Select

        Dim xlPackage As SpreadsheetDocument = SpreadsheetDocument.Create(file_path, wb_type)

        'add WB xml file
        Dim wbPart As WorkbookPart = xlPackage.AddWorkbookPart

        'append workbook node to workbookpart xml file
        wbPart.Workbook = New Workbook

        'add worksheet xml file
        Dim wsPart As WorksheetPart = wbPart.AddNewPart(Of WorksheetPart)()

        'append worksheet/sheetdata nodes to worksheetpart xml file
        wsPart.Worksheet = New Worksheet(New SheetData())

        'add sheets collection
        Dim sheets As Sheets = xlPackage.WorkbookPart.Workbook.AppendChild(Of Sheets)(New Sheets())

        'add the sheet
        Dim ws As Sheet = New Sheet

        'relate it to the worksheetpart
        ws.Id = xlPackage.WorkbookPart.GetIdOfPart(wsPart)

        'assign the id and name
        ws.SheetId = 1
        ws.Name = sheet_nm

        'append the single sheet to the sheets collection
        sheets.Append(ws)

        'save and close
        wbPart.Workbook.Save()
        xlPackage.Close()
    End Sub

    Shared Function add_bold_index(xlpackage As SpreadsheetDocument) As Integer

        Dim bold_font_index As Integer = -1

        Dim did_style_sheet_exist As Boolean

        Dim stylePart = xlpackage.WorkbookPart.WorkbookStylesPart

        If stylePart Is Nothing Then
            did_style_sheet_exist = False
            stylePart = xlpackage.WorkbookPart.AddNewPart(Of WorkbookStylesPart)()
        Else
            did_style_sheet_exist = True
        End If

        'if the style sheet already exists, we just need to add a new font with bold

        If did_style_sheet_exist Then
            Dim sSheet = stylePart.Stylesheet

            'Get the default font (style 0)
            Dim first_font = sSheet.Fonts.FirstOrDefault

            'does the default go 
            'get the properties
            Dim fSize As String = first_font.Descendants(Of FontSize).FirstOrDefault.Val
            Dim color_theme As String = first_font.Descendants(Of DocumentFormat.OpenXml.Spreadsheet.Color).FirstOrDefault.Theme
            Dim color_rgb As String = first_font.Descendants(Of DocumentFormat.OpenXml.Spreadsheet.Color).FirstOrDefault.Rgb

            Dim fName As String = first_font.Descendants(Of FontName).FirstOrDefault.Val

            'Check existing fonts to see if we have a match, if so, we are done
            Dim loop_index As Integer = 0
            For Each f As Spreadsheet.Font In sSheet.Fonts

                If f.FontSize.Val = fSize And f.Color.Theme = color_theme And f.Color.Rgb = color_rgb And f.Elements(Of Bold).FirstOrDefault IsNot Nothing Then

                    bold_font_index = loop_index
                    Exit For
                End If
                loop_index += 1
            Next

            'if we did not get the bold font index, add it
            If bold_font_index = -1 Then
                'append the new font with a bold
                Dim fnew As New DocumentFormat.OpenXml.Spreadsheet.Font( 'Our bold headers ix 1
                            New Bold(),
                            New FontSize() With {.Val = fSize},
                            New DocumentFormat.OpenXml.Spreadsheet.Color() With {.Rgb = color_rgb},
                            New FontName() With {.Val = fName},
                            New FontFamilyNumbering() With {.Val = 2}
                        )
                sSheet.Fonts.Append(fnew)
                'New Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},

                bold_font_index = (sSheet.Fonts.Elements(Of DocumentFormat.OpenXml.Spreadsheet.Font).Distinct.Count - 1).ToString

                Dim cF As New CellFormat() With {.FontId = bold_font_index, .FillId = 0, .BorderId = 0, .ApplyFont = True}

                sSheet.CellFormats.Append(cF)
            End If



            '------------------------------------------------------------------------------------------------------------------------
            'append a datestyle format as well

            'Dim cfDate As New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0, .NumberFormatId = 14, .ApplyNumberFormat = True}

            'sSheet.CellFormats.Append(cfDate)

            '_date_style_index = (sSheet.CellFormats.Elements(Of CellFormat).Distinct.Count - 1).ToString

        Else

            bold_font_index = 1

            stylePart.Stylesheet = New Stylesheet(
               New Fonts(
                    New DocumentFormat.OpenXml.Spreadsheet.Font( 'default
                        New FontSize() With {.Val = 11},
                        New DocumentFormat.OpenXml.Spreadsheet.Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
                        New FontName() With {.Val = "Calibri"},
                        New FontFamilyNumbering() With {.Val = 2}
                    ),
                    New DocumentFormat.OpenXml.Spreadsheet.Font( 'Our bold headers ix 1
                        New Bold(),
                        New FontSize() With {.Val = 11},
                        New DocumentFormat.OpenXml.Spreadsheet.Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
                        New FontName() With {.Val = "Calibri"},
                        New FontFamilyNumbering() With {.Val = 2}
                    )
                ),
               New Fills(
                    New Fill(New PatternFill() With {.PatternType = PatternValues.None}) 'default
                ),
               New Borders(
                    New Border( 'default
                        New LeftBorder(),
                        New RightBorder(),
                        New TopBorder(),
                        New BottomBorder(),
                        New DiagonalBorder()
                    )
                ),
               New CellFormats(
                    New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0}, ' (index 0: default)
                    New CellFormat() With {.FontId = bold_font_index, .FillId = 0, .BorderId = 0, .ApplyFont = True}, 'bold (index 1)
                    New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0, .NumberFormatId = 14, .ApplyNumberFormat = True} 'Shortdate (index 2) 'for short dates in MM/dd/yyyy (numberFormatID 14 is shortdate)
                )
            )
        End If

        Return bold_font_index

    End Function

    Private Shared Function get_cell_val_width(font_nm As System.Drawing.Font, cell_val As String, using_auto_filter As Boolean) As Double

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

    Private Class export_col
        Property col_nm As String
        Property ord_pos As Integer
        Property data_type As String
        Property xl_dataType As CellValues

        Property cell_width As Double
    End Class


End Class
