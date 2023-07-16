Imports DocumentFormat.OpenXml.Spreadsheet
Imports DocumentFormat.OpenXml.Packaging
Imports DocumentFormat.OpenXml.OpenXmlReader
Imports DocumentFormat.OpenXml.OpenXmlWriter

Public Class cExcel
    Shared xl_err_msg As String = ""

    '-------------------------------------------------------------------------------------
    'Tasks
    '   [1] Create Workbook - done
    '   [2] Add Worksheet - done
    '   [3] Delete Worksheet
    '   [4] Rename Worksheet - done
    '   [5] Clear Worksheet - done
    '   [6] Import Data - coding half way complete...need to make dynamic
    '   [7] Export Data - done


    Shared Sub create_workbook(file_path As String, Optional sheet_nm As String = "Sheet1")

        Try
            Dim wb_type As DocumentFormat.OpenXml.SpreadsheetDocumentType

            Select Case Right(file_path, 4).ToLower
                Case "xlsx"
                    wb_type = DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook
                Case "xlsm"
                    wb_type = DocumentFormat.OpenXml.SpreadsheetDocumentType.MacroEnabledWorkbook
            End Select

            Dim wb As SpreadsheetDocument = SpreadsheetDocument.Create(file_path, wb_type)

            'add wb part
            Dim wbPart As WorkbookPart = wb.AddWorkbookPart
            wbPart.Workbook = New Workbook

            'add ws part
            Dim wsPart As WorksheetPart = wbPart.AddNewPart(Of WorksheetPart)()
            wsPart.Worksheet = New Worksheet(New SheetData())

            'add sheets collection
            Dim sheets As Sheets = wb.WorkbookPart.Workbook.AppendChild(Of Sheets)(New Sheets())

            'add the worksheet
            Dim ws As Sheet = New Sheet
            ws.Id = wb.WorkbookPart.GetIdOfPart(wsPart)

            ws.SheetId = 1
            ws.Name = sheet_nm

            sheets.Append(ws)

            wbPart.Workbook.Save()
            wb.Close()

        Catch ex As Exception
            xl_err_msg = ex.Message.ToString

        End Try

    End Sub

    Shared Function add_worksheet(workbook As SpreadsheetDocument, sheet_nm As String) As Sheet

        Try
            Dim sheet As Sheet
            Using workbook
                Dim wbPart As WorkbookPart = workbook.WorkbookPart

                Dim ws As Sheet = wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()
                If ws IsNot Nothing Then
                    Throw New ArgumentException("Sheet [" + sheet_nm + "] already exists")
                End If

                Dim wspart As WorksheetPart = workbook.WorkbookPart.AddNewPart(Of WorksheetPart)
                wspart.Worksheet = New Worksheet(New SheetData())

                Dim sheets As Sheets = workbook.WorkbookPart.Workbook.GetFirstChild(Of Sheets)
                Dim relId As String = workbook.WorkbookPart.GetIdOfPart(wspart)

                Dim sheet_id As UInteger = 1
                If sheets.Elements(Of Sheet).Count > 0 Then
                    sheet_id = sheets.Elements(Of Sheet).Select(Function(s) s.SheetId.Value).Max + 1
                End If

                sheet = New Sheet
                sheet.Id = relId
                sheet.SheetId = sheet_id
                sheet.Name = sheet_nm
                sheets.Append(sheet)

            End Using

            Return sheet

        Catch ex As Exception
            xl_err_msg = ex.Message.ToString
            Return Nothing
        End Try

    End Function

    Shared Function set_worksheet(workbook As SpreadsheetDocument, sheet_nm As String) As Sheet

        Try
            Dim sheet As Sheet
            Using workbook
                Dim wbPart As WorkbookPart = workbook.WorkbookPart

                sheet = wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()
                If sheet Is Nothing Then
                    Throw New ArgumentException("Sheet [" + sheet_nm + "] does not exist")
                End If

            End Using

            Return sheet

        Catch ex As Exception
            xl_err_msg = ex.Message.ToString
            Return Nothing
        End Try

    End Function


    Shared Sub rename_worksheet(file_path As String, curr_sheet_nm As String, new_sheet_nm As String)

        Using wb As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)
            Dim wbPart As WorkbookPart = wb.WorkbookPart

            Dim ws As Sheet = wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = curr_sheet_nm.ToLower).FirstOrDefault()
            If ws Is Nothing Then
                Throw New ArgumentException("Sheet [" + curr_sheet_nm + "] does not exist")
            End If

            ws.Name = new_sheet_nm

        End Using

    End Sub

    Shared Sub clear_worksheet(file_path As String, sheet_nm As String)
        Using wb As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)
            Dim wbPart As WorkbookPart = wb.WorkbookPart

            Dim ws As Sheet = wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()
            If ws Is Nothing Then
                Throw New ArgumentException("Sheet [" + sheet_nm + "] does not exist")
            End If

            Dim wsPart As WorksheetPart = CType(wbPart.GetPartById(ws.Id), WorksheetPart)

            Dim sheetData As SheetData = wsPart.Worksheet.Elements(Of SheetData)().First()

            sheetData.RemoveAllChildren()

        End Using
    End Sub


    Private Shared ReadOnly _sharedStrings As New Dictionary(Of String, Integer)
    Private Shared _sharedStringIndex As Integer = 0

    'To write out data to xlsx and create it on the fly
    'not configured to append to existing spreadsheet

    Shared Sub export_data()
        Dim file_path As String = "C:\temp\xlOut.xlsx"
        Dim sheet_nm As String = "Sheet1"

        Dim bold_headers As Boolean = True


        If IO.File.Exists(file_path) Then
            IO.File.Delete(file_path)
        End If

        Using wb As SpreadsheetDocument = SpreadsheetDocument.Create(file_path, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook)

            Dim wbPart As WorkbookPart = wb.AddWorkbookPart
            wbPart.Workbook = New Workbook

            'create the style sheet
            createStyleSheet(wbPart)

            'add worksheet part
            Dim wsPart As WorksheetPart = wbPart.AddNewPart(Of WorksheetPart)()

            'This gets added when we create the openxml writer
            'wsPart.Worksheet = New Worksheet(New SheetData())

            'add sheets collection
            Dim sheets As Sheets = wb.WorkbookPart.Workbook.AppendChild(Of Sheets)(New Sheets())

            'add sheet
            Dim sh As Sheet = New Sheet() With {.Id = wb.WorkbookPart.GetIdOfPart(wsPart), .SheetId = 1, .Name = sheet_nm}
            sheets.Append(sh)

            Dim writer As DocumentFormat.OpenXml.OpenXmlWriter = DocumentFormat.OpenXml.OpenXmlWriter.Create(wsPart)

            Using cn As New SqlClient.SqlConnection("Server= MDDT\DEV16; Integrated Security=  True")
                cn.Open()
                Dim cmd As New SqlClient.SqlCommand
                cmd.Connection = cn
                cmd.CommandText = "Select * From dfdm.dbo.lkup_dc"
                cmd.CommandText = "select top 10000 * from adventureworks.Production.TransactionHistory"
                cmd.CommandType = CommandType.Text

                Dim oxa As List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

                'create the worksheet and sheetdata for the Sheet
                'this creates the sheet.xml file abd starts the writetag for the worksheet
                writer.WriteStartElement(New Worksheet())
                'starts the writetag for sheetdata
                writer.WriteStartElement(New SheetData())

                'counter for rows
                Dim counter As Integer = 1

                Using reader = cmd.ExecuteReader

                    '----------------------------------------------------------------------------
                    'Add Headers

                    oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

                    'add row index : NOT required...leaving out for compression purposes
                    'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, counter.ToString))
                    'writer.WriteStartElement(New Row(), oxa)
                    writer.WriteStartElement(New Row())

                    'Add field names
                    For i = 0 To reader.FieldCount - 1

                        Dim cell_val As String = reader.GetName(i)
                        Dim cell_ref As String = getXLColLetterFromNumber(i + 1) + counter.ToString

                        Console.WriteLine("Column: {0}; Format: {1}", cell_val, reader.GetDataTypeName(i).ToString)

                        writeCellVal(writer, cell_val, cell_ref, "varchar", bold_headers)

                    Next
                    Exit Sub

                    'close header row
                    writer.WriteEndElement()

                    'advance to next set of rows
                    counter += 1

                    '--------------------------------------------------------------------------------------------------------------------------
                    'Add Data

                    While reader.Read

                        oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

                        'add row index : NOT required...leaving out for compression purposes
                        'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, counter.ToString))
                        'writer.WriteStartElement(New Row(), oxa)
                        writer.WriteStartElement(New Row())

                        'add field values
                        For i = 0 To reader.FieldCount - 1

                            Dim cell_val As String = reader(i).ToString
                            Dim cell_ref As String = getXLColLetterFromNumber(i + 1) + counter.ToString
                            Dim data_type As String = reader.GetDataTypeName(i).ToString.ToLower

                            writeCellVal(writer, cell_val, cell_ref, data_type, False)

                        Next

                        'close row
                        writer.WriteEndElement()

                        counter += 1

                    End While

                    'close sheetdata tag
                    writer.WriteEndElement()
                    'close worksheet tag
                    writer.WriteEndElement()

                End Using

            End Using

            'saves the .xml file
            writer.Close()

            '------------------------------------------------------------------------------------------------------
            'Add Shared Strings Catalog

            If _sharedStringIndex > 0 Then

                Dim sharedStrings = wbPart.AddNewPart(Of SharedStringTablePart)

                Using ssWriter As DocumentFormat.OpenXml.OpenXmlWriter = DocumentFormat.OpenXml.OpenXmlWriter.Create(sharedStrings)

                    ssWriter.WriteStartElement(New SharedStringTable)
                    For Each itm In _sharedStrings
                        ssWriter.WriteStartElement(New SharedStringItem())
                        ssWriter.WriteElement(New Text(itm.Key))
                        ssWriter.WriteEndElement()
                    Next

                    ssWriter.WriteEndElement()

                End Using 'this auto closes the writer...no need to explicitly tell it

            End If

        End Using


    End Sub


    Shared Sub writeCellVal(writer As DocumentFormat.OpenXml.OpenXmlWriter, cell_val As String, cellRef As String, data_type As String, apply_bold As Boolean)

        Dim oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

        'set the reference attribute for the cell : Not required...we are leaving out for compression purposes
        'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, cellRef))

        If apply_bold Then
            oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("s", Nothing, "1"))
        End If

        Select Case data_type
            Case "int", "float", "smallint", "date"
                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "n"))

                If data_type = "date" And Trim(cell_val) <> "" Then

                    'style for short date
                    oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("s", Nothing, "2"))

                    'convert the date to excel number
                    Dim dt As Date = CDate(cell_val)
                    Dim dtInt = CInt((dt.[Date] - New DateTime(1900, 1, 1)).TotalDays) + 2

                    cell_val = dtInt.ToString

                End If

                'write the cell
                writer.WriteStartElement(New Cell(), oxa)
                writer.WriteElement(New CellValue(cell_val))
                writer.WriteEndElement()

            Case "boolean"
                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "b"))

                'write the cell
                writer.WriteStartElement(New Cell(), oxa)

                If cell_val.ToLower = "true" Then
                    cell_val = "1"
                Else
                    cell_val = "0"
                End If

                writer.WriteElement(New CellValue(cell_val))
                writer.WriteEndElement()

            Case Else

                'shared string
                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "s"))

                'add to shared string array if not exists
                If Not _sharedStrings.ContainsKey(cell_val) Then
                    _sharedStrings.Add(cell_val, _sharedStringIndex)
                    _sharedStringIndex += 1
                End If

                writer.WriteStartElement(New Cell(), oxa)
                writer.WriteElement(New CellValue(_sharedStrings(cell_val).ToString()))
                writer.WriteEndElement()

        End Select
    End Sub

    Shared Sub createStyleSheet(wbPart As WorkbookPart)

        Dim stylePart As WorkbookStylesPart = wbPart.AddNewPart(Of WorkbookStylesPart)()

        stylePart.Stylesheet = New Stylesheet(
           New Fonts(
                New Font( 'default
                    New FontSize() With {.Val = 11},
                    New Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
                    New FontName() With {.Val = "Calibri"}
                ),
                New Font( 'Our bold headers ix 1
                    New Bold(),
                    New FontSize() With {.Val = 11},
                    New Color() With {.Rgb = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
                    New FontName() With {.Val = "Calibri"}
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
                New CellFormat() With {.FontId = 1, .FillId = 0, .BorderId = 0, .ApplyFont = True}, 'bold (index 1)
                New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0, .NumberFormatId = 14, .ApplyNumberFormat = True} 'Shortdate (index 2) 'for short dates in MM/dd/yyyy (numberFormatID 14 is shortdate)
            )
        )



    End Sub


    Shared Sub import_excel_sheet()

        Dim file_path As String = "C:\temp\XLLarge.xlsx"
        Dim sheet_nm As String = "Sheet1"
        Dim sheet_id As UInt32
        Dim batch_size As Integer = 10000

        Using wb As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)

            Dim wbPart As WorkbookPart = wb.WorkbookPart

            Dim ws As Sheet = wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name = sheet_nm).FirstOrDefault()
            If ws IsNot Nothing Then
                sheet_id = ws.SheetId
            Else
                Throw New ArgumentException("Sheet [" + sheet_nm + "] does not exist")
            End If

            Dim wsPart As WorksheetPart = CType(wbPart.GetPartById(ws.Id), WorksheetPart)

            Dim stringTable = wbPart.GetPartsOfType(Of SharedStringTablePart).FirstOrDefault()

            'PERFORMANCE ISSUE: when a large sheet has a lot of shared strings, the initial reads are quick but as you go further down,they get exponentially slower
            'this is because the shared string is an enumrable lookup
            'to get around it, load as an array, then lookup that way for the cell val

            'https://social.msdn.microsoft.com/Forums/office/en-US/79752491-ddf7-4620-8930-d526d8d66164/openxmlreader-sax-reader-appears-to-slow-down-as-it-gradually-reads-large-file?forum=oxmlsdk

            'Convert the shared strings to an array
            Dim sharedStringItemsArray As SharedStringItem() = stringTable.SharedStringTable.Elements(Of SharedStringItem)().ToArray()

            Dim sheetData As SheetData = wsPart.Worksheet.Elements(Of SheetData)().First()


            '---------------------------------------------------------------------------------------------
            'read the columns in the first row and create the data table

            Dim import_data_table As New DataTable

            Dim first_row As Row = sheetData.Elements(Of Row)().FirstOrDefault()

            'sheetData.Elements(Of Row).ElementAt(1)

            For Each c As Cell In first_row.Elements(Of Cell)()

                Dim col_nm As String = get_cell_val(c, sharedStringItemsArray)

                import_data_table.Columns.Add(col_nm, GetType(String))

            Next



            Dim cVal As String

            Dim counter As Integer = 1
            Dim current_batch As Integer = 0
            Dim bulk_insert_count As Integer = 1
            Dim row_nbr As Integer = 1

            'http://stackoverflow.com/questions/24829801/reading-a-large-excel-file-by-openxml
            'https://msdn.microsoft.com/en-us/library/office/gg575571.aspx
            'http://stackoverflow.com/questions/4183195/get-excel-cell-value-with-row-and-column-position-through-open-xml-sdk-linq-quer

            'this gets exponentially slower as more data is read....
            Dim reader As DocumentFormat.OpenXml.OpenXmlReader = DocumentFormat.OpenXml.OpenXmlReader.Create(wsPart)
            While reader.Read()

                If reader.ElementType = GetType(Row) Then
                    reader.ReadFirstChild()

                    current_batch += 1
                    Dim dRow As DataRow = import_data_table.NewRow

                    Dim col_pos As Integer = 0

                    Do
                        If reader.ElementType = GetType(Cell) Then

                            Dim c = DirectCast(reader.LoadCurrentElement(), Cell)
                            cVal = get_cell_val(c, sharedStringItemsArray)
                            dRow(col_pos) = cVal
                            col_pos += 1
                        End If
                    Loop While reader.ReadNextSibling()

                    'if we are on the first row, thats the headers...skip it
                    If row_nbr >= 2 Then
                        import_data_table.Rows.Add(dRow)
                    End If
                    row_nbr += 1
                End If


                If current_batch >= batch_size Then
                    Console.WriteLine("bulk loading batch {0}", bulk_insert_count)
                    bulkLoad(import_data_table)
                    current_batch = 0
                    bulk_insert_count += 1
                End If

                counter += 1

            End While

            'Load last batch if we didnt
            If current_batch > 0 Then
                Console.WriteLine("bulk loading batch {0}", bulk_insert_count)
                bulkLoad(import_data_table)
                current_batch = 0
                bulk_insert_count += 1
            End If

        End Using


    End Sub

    Private Shared Function getXLColLetterFromNumber(colIndex As Integer) As String
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

    Shared Sub bulkLoad(ByVal tbl As DataTable)

        Using cn As New SqlClient.SqlConnection("Server = DFSQL; Integrated Security = True")
            cn.Open()

            Using bc As SqlClient.SqlBulkCopy = New SqlClient.SqlBulkCopy(cn)
                bc.DestinationTableName = "##tmp123mm"
                bc.WriteToServer(tbl)
            End Using

        End Using

        'clear the table and free up space
        tbl.Rows.Clear()

    End Sub

    Shared Function get_cell_val(c As Cell, stringArray As SharedStringItem()) As String

        Dim val As String

        val = c.CellValue.InnerText

        If c.DataType IsNot Nothing Then
            Select Case c.DataType.Value
                Case CellValues.SharedString
                    Dim ssi As SharedStringItem = stringArray(Integer.Parse(c.CellValue.InnerText))
                    val = ssi.InnerText
                    'val = stringtable.SharedStringTable.ElementAt(Integer.Parse(c.InnerText)).InnerText
                Case CellValues.Boolean
                    Select Case val
                        Case "0"
                            val = "FALSE"
                        Case Else
                            val = "TRUE"
                    End Select
            End Select
        End If

        Return val

    End Function

    'works if the sheet i'm deleting references other sheets....
    'does not work if sheet i'm deleting is referenced by another sheet
    Shared Sub delete_worksheet(file_path As String, sheet_nm As String)
        Using wb As SpreadsheetDocument = SpreadsheetDocument.Open(file_path, True)

            Dim wbPart As WorkbookPart = wb.WorkbookPart

            Dim ws As Sheet = wbPart.Workbook.Descendants(Of Sheet).Where(Function(s) s.Name.ToString.ToLower = sheet_nm.ToLower).FirstOrDefault()
            If ws Is Nothing Then
                Throw New ArgumentException("Sheet [" + sheet_nm + "] does not exist")
            End If

            Dim sheet_id As UInt32 = ws.SheetId

            Dim wsPart As WorksheetPart = CType(wbPart.GetPartById(ws.Id), WorksheetPart)

            '------------------------------------------------------------------
            'remove pivottable stuff

            Dim pvtCacheParts As IEnumerable(Of PivotTableCacheDefinitionPart) = wbPart.PivotTableCacheDefinitionParts
            Dim pvtCacheDefPartColl As New Dictionary(Of PivotTableCacheDefinitionPart, String)()
            For Each Item As PivotTableCacheDefinitionPart In pvtCacheParts
                Dim pvtCacheDef As PivotCacheDefinition = Item.PivotCacheDefinition
                Dim pvtCahce = pvtCacheDef.Descendants(Of CacheSource)().Where(Function(s) s.WorksheetSource.Sheet = sheet_nm)
                If pvtCahce.Count() > 0 Then
                    pvtCacheDefPartColl.Add(Item, Item.ToString())
                End If
            Next
            For Each Item In pvtCacheDefPartColl
                wbPart.DeletePart(Item.Key)
            Next

            '------------------------------------------------------------------
            'remove sheet ref as worksheet part

            ws.Remove()
            wbPart.DeletePart(wsPart)

            '------------------------------------------------------------------
            'remove defined names

            Dim defNames = wbPart.Workbook.Descendants(Of DefinedNames)().FirstOrDefault()
            If defNames IsNot Nothing Then
                Dim defNamesToDelete As New List(Of DefinedName)()

                For Each itm As DefinedName In defNames
                    If itm.Text.Contains(sheet_nm + "!").ToString.ToLower Then
                        defNamesToDelete.Add(itm)
                    End If
                Next

                For Each itm As DefinedName In defNamesToDelete
                    itm.Remove()
                Next
            End If

            '------------------------------------------------------------------
            'remove calc chain

            'for this to work...need to get existing cell refences in all other sheets that link to the sheet to delete and update them
            'see this: https://social.msdn.microsoft.com/Forums/office/en-US/68950700-4c1d-43c2-99b7-782ce3abc6a4/remove-cellformula-using-openxml?forum=oxmlsdk


            Dim calChainPart As CalculationChainPart
            calChainPart = wbPart.CalculationChainPart
            If calChainPart IsNot Nothing Then

                'For Each entry In calChainPart.CalculationChain.Descendants(Of CalculationCell)
                '    Dim x = entry.SheetId.ToString
                '    Dim y = "z"
                'Next



                Dim cEntries = calChainPart.CalculationChain.Descendants(Of CalculationCell).Where(Function(s) s.SheetId.ToString = sheet_id.ToString)


                Dim calcsToDelete As New List(Of CalculationCell)()
                For Each itm As CalculationCell In cEntries
                    calcsToDelete.Add(itm)
                Next

                For Each Item As CalculationCell In calcsToDelete
                    Item.Remove()
                Next

                If calChainPart.CalculationChain.Count() = 0 Then
                    wbPart.DeletePart(calChainPart)
                End If
            End If


            wb.WorkbookPart.Workbook.Save()

        End Using
    End Sub

End Class
