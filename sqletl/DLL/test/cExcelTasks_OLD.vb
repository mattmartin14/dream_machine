'Imports DocumentFormat.OpenXml.Spreadsheet
'Imports DocumentFormat.OpenXml.Packaging
'Class cExcelTasks

'    'Shared vars for when exporting
'    Private Shared ReadOnly _sharedStrings As New Dictionary(Of String, Integer)
'    Private Shared _sharedStringIndex As Integer = 0


'    Shared Sub main(ByRef err_msg As String)

'        Dim task As String = LCase(EDIS.get_edis_pval("sub_task"))

'        Select Case task
'            Case "get_sheet_list"
'                get_sheet_list(err_msg)
'            Case "export_data"
'                export_data(err_msg)
'        End Select
'    End Sub

'    Shared Sub export_data(ByRef err_msg As String)

'        Try
'            Dim file_path As String = EDIS.get_edis_pval("file_path")
'            Dim sheet_nm As String = EDIS.get_edis_pval("sheet_nm")
'            Dim src_qry As String = EDIS.get_edis_pval("src_qry")
'            Dim bold_headers As Boolean = CBool(EDIS.get_edis_pval("bold_headers"))

'            If IO.File.Exists(file_path) Then
'                IO.File.Delete(file_path)
'            End If

'            Using XLPackage As SpreadsheetDocument = SpreadsheetDocument.Create(file_path, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook)

'                Dim wbPart As WorkbookPart = XLPackage.AddWorkbookPart
'                wbPart.Workbook = New Workbook

'                'create the style sheet
'                createStyleSheet(wbPart)

'                'add worksheet part
'                Dim wsPart As WorksheetPart = wbPart.AddNewPart(Of WorksheetPart)()

'                'This gets added when we create the openxml writer
'                'wsPart.Worksheet = New Worksheet(New SheetData())

'                'add sheets collection
'                Dim sheets As Sheets = XLPackage.WorkbookPart.Workbook.AppendChild(Of Sheets)(New Sheets())

'                'add sheet
'                Dim sh As Sheet = New Sheet() With {.Id = XLPackage.WorkbookPart.GetIdOfPart(wsPart), .SheetId = 1, .Name = sheet_nm}
'                sheets.Append(sh)

'                Using writer As DocumentFormat.OpenXml.OpenXmlWriter = DocumentFormat.OpenXml.OpenXmlWriter.Create(wsPart)

'                    Using cn As New SqlClient.SqlConnection("Server= " + EDIS.mssql_inst + "; Integrated Security=  True")
'                        cn.Open()

'                        Using cmd As New SqlClient.SqlCommand(src_qry, cn)
'                            cmd.CommandTimeout = 0

'                            Dim oxa As List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'                            'create the worksheet and sheetdata for the Sheet
'                            'this creates the sheet.xml file abd starts the writetag for the worksheet
'                            writer.WriteStartElement(New Worksheet())
'                            'starts the writetag for sheetdata
'                            writer.WriteStartElement(New SheetData())

'                            'counter for rows
'                            Dim counter As Integer = 1

'                            Using reader = cmd.ExecuteReader

'                                '----------------------------------------------------------------------------
'                                'Add Headers

'                                oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'                                'add row index : NOT required...leaving out for compression purposes
'                                'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, counter.ToString))
'                                'writer.WriteStartElement(New Row(), oxa)
'                                writer.WriteStartElement(New Row())

'                                'Add field names
'                                For i = 0 To reader.FieldCount - 1

'                                    Dim cell_val As String = reader.GetName(i)
'                                    Dim cell_ref As String = cExcelFunctions.getXLColLetterFromNumber(i + 1) + counter.ToString

'                                    writeCellVal(writer, cell_val, cell_ref, "varchar", bold_headers)

'                                Next

'                                'close header row
'                                writer.WriteEndElement()

'                                'advance to next set of rows
'                                counter += 1

'                                '--------------------------------------------------------------------------------------------------------------------------
'                                'Add Data

'                                While reader.Read

'                                    oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'                                    'add row index : NOT required...leaving out for compression purposes
'                                    'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, counter.ToString))
'                                    'writer.WriteStartElement(New Row(), oxa)
'                                    writer.WriteStartElement(New Row())

'                                    'add field values
'                                    For i = 0 To reader.FieldCount - 1

'                                        Dim cell_val As String = reader(i).ToString
'                                        Dim cell_ref As String = cExcelFunctions.getXLColLetterFromNumber(i + 1) + counter.ToString
'                                        Dim data_type As String = reader.GetDataTypeName(i).ToString.ToLower

'                                        writeCellVal(writer, cell_val, cell_ref, data_type, False)

'                                    Next

'                                    'close row
'                                    writer.WriteEndElement()

'                                    counter += 1

'                                End While

'                                'close sheetdata tag
'                                writer.WriteEndElement()
'                                'close worksheet tag
'                                writer.WriteEndElement()

'                            End Using 'close sql reader

'                        End Using 'close cmd

'                    End Using 'close connection to sql

'                    'saves the .xml file / end using statement does the close to save
'                    'writer.Close()

'                End Using 'close writer
'                '------------------------------------------------------------------------------------------------------
'                'Add Shared Strings Catalog

'                If _sharedStringIndex > 0 Then

'                    Dim sharedStrings = wbPart.AddNewPart(Of SharedStringTablePart)

'                    Using ssWriter As DocumentFormat.OpenXml.OpenXmlWriter = DocumentFormat.OpenXml.OpenXmlWriter.Create(sharedStrings)

'                        ssWriter.WriteStartElement(New SharedStringTable)
'                        For Each itm In _sharedStrings
'                            ssWriter.WriteStartElement(New SharedStringItem())
'                            ssWriter.WriteElement(New Text(itm.Key))
'                            ssWriter.WriteEndElement()
'                        Next

'                        ssWriter.WriteEndElement()

'                    End Using 'this auto closes the writer...no need to explicitly tell it

'                End If

'            End Using 'close XLPackage


'        Catch ex As Exception
'            err_msg = ex.Message.ToString
'        End Try


'    End Sub


'    Shared Sub createStyleSheet(wbPart As WorkbookPart)

'        Dim stylePart As WorkbookStylesPart = wbPart.AddNewPart(Of WorkbookStylesPart)()

'        stylePart.Stylesheet = New Stylesheet(
'           New Fonts(
'                New Font( 'default
'                    New FontSize() With {.Val = 11},
'                    New Color() With {.RGB = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
'                    New FontName() With {.Val = "Calibri"}
'                ),
'                New Font( 'Our bold headers ix 1
'                    New Bold(),
'                    New FontSize() With {.Val = 11},
'                    New Color() With {.RGB = New DocumentFormat.OpenXml.HexBinaryValue() With {.Value = "00000"}},
'                    New FontName() With {.Val = "Calibri"}
'                )
'            ),
'           New Fills(
'                New Fill(New PatternFill() With {.PatternType = PatternValues.None}) 'default
'            ),
'           New Borders(
'                New Border( 'default
'                    New LeftBorder(),
'                    New RightBorder(),
'                    New TopBorder(),
'                    New BottomBorder(),
'                    New DiagonalBorder()
'                )
'            ),
'           New CellFormats(
'                New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0}, ' (index 0: default)
'                New CellFormat() With {.FontId = 1, .FillId = 0, .BorderId = 0, .ApplyFont = True}, 'bold (index 1)
'                New CellFormat() With {.FontId = 0, .FillId = 0, .BorderId = 0, .NumberFormatId = 14, .ApplyNumberFormat = True} 'Shortdate (index 2) 'for short dates in MM/dd/yyyy (numberFormatID 14 is shortdate)
'            )
'        )



'    End Sub

'    Shared Sub writeCellVal(writer As DocumentFormat.OpenXml.OpenXmlWriter, cell_val As String, cellRef As String, data_type As String, apply_bold As Boolean)

'        Dim oxa = New List(Of DocumentFormat.OpenXml.OpenXmlAttribute)

'        'set the reference attribute for the cell : Not required...we are leaving out for compression purposes
'        'oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("r", Nothing, cellRef))

'        If apply_bold Then
'            oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("s", Nothing, "1"))
'        End If

'        Select Case data_type
'            Case "int", "float", "smallint", "date", "decimal", "numeric", "bigint", "tinyint", "money"
'                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "n"))

'                If data_type = "date" And Trim(cell_val) <> "" Then

'                    'style for short date
'                    oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("s", Nothing, "2"))

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

'                'shared string
'                oxa.Add(New DocumentFormat.OpenXml.OpenXmlAttribute("t", Nothing, "s"))

'                'add to shared string array if not exists
'                If Not _sharedStrings.ContainsKey(cell_val) Then
'                    _sharedStrings.Add(cell_val, _sharedStringIndex)
'                    _sharedStringIndex += 1
'                End If

'                writer.WriteStartElement(New Cell(), oxa)
'                writer.WriteElement(New CellValue(_sharedStrings(cell_val).ToString()))
'                writer.WriteEndElement()

'        End Select
'    End Sub

'    Private Shared Sub get_sheet_list(ByRef err_msg As String)

'        Try
'            Dim file_path As String = EDIS.get_edis_pval("file_path")
'            Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")

'            Dim cn_str As String = mCn_builder.build_cn_str("EXCEL", file_path, True)

'            Using XLcn As New OleDb.OleDbConnection(cn_str)
'                XLcn.Open()
'                Dim oda As DataTable = XLcn.GetOleDbSchemaTable(OleDb.OleDbSchemaGuid.Tables, Nothing)

'                Using sqlCn As New SqlClient.SqlConnection("server = " + EDIS.mssql_inst + "; Integrated Security = True")
'                    sqlCn.Open()
'                    For Each sheet In oda.Rows
'                        Using sqlcmd As New SqlClient.SqlCommand("INSERT INTO [" + output_tbl_nm + "] VALUES (@sheet_nm)", sqlCn)
'                            sqlcmd.Parameters.AddWithValue("@sheet_nm", sheet("TABLE_NAME").ToString)
'                            sqlcmd.ExecuteNonQuery()
'                        End Using
'                    Next
'                    sqlCn.Close()
'                End Using
'                XLcn.Close()
'            End Using
'        Catch ex As Exception
'            err_msg = ex.Message.ToString
'        End Try
'    End Sub


'End Class
