'Imports DocumentFormat.OpenXml.Spreadsheet
'Imports DocumentFormat.OpenXml.Packaging
'Friend Class cExcelFunctions_OLD


'    Shared Function getXLColLetterFromNumber(colIndex As Integer) As String
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

'    'Shared Function get_cell_val(c As Cell, stringArray As SharedStringItem()) As String

'    '    Dim val As String

'    '    val = c.CellValue.InnerText

'    '    If c.DataType IsNot Nothing Then
'    '        Select Case c.DataType.Value
'    '            Case CellValues.SharedString
'    '                Dim ssi As SharedStringItem = stringArray(Integer.Parse(c.CellValue.InnerText))
'    '                val = ssi.InnerText
'    '                'val = stringtable.SharedStringTable.ElementAt(Integer.Parse(c.InnerText)).InnerText
'    '            Case CellValues.Boolean
'    '                Select Case val
'    '                    Case "0"
'    '                        val = "FALSE"
'    '                    Case Else
'    '                        val = "TRUE"
'    '                End Select
'    '        End Select
'    '    End If

'    '    Return val

'    'End Function

'End Class
