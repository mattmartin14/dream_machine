
Imports System.Data.SqlClient
Imports System.Data.OleDb
Imports System.Data.Odbc
Imports System.IO

Module dft_functions

    Sub write_ms_template_file_out(ByVal ms_file_type As String, ByVal file_path As String, ByRef err_msg As String)
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

    Sub run_mssql_cmd(ByVal sql_cmd As String, ByVal cn_str As String, ByRef err_msg As String)

        Try
            Using cn As New SqlConnection(cn_str)
                cn.Open()
                Using cmd As New SqlCommand(sql_cmd, cn)
                    cmd.ExecuteNonQuery()
                End Using
                cn.Close()
            End Using
        Catch ex As SqlException
            err_msg = ex.Message.ToString()
        End Try

    End Sub

    Sub run_odbc_cmd(ByVal odbc_cmd As String, ByVal cn_str As String, ByRef err_msg As String)
        Try
            Using cn As New OdbcConnection(cn_str)
                cn.Open()
                Using cmd As New OdbcCommand(odbc_cmd, cn)
                    cmd.ExecuteNonQuery()
                End Using
                cn.Close()
            End Using
        Catch ex As Odbc.OdbcException
            err_msg = ex.Message.ToString
        End Try
    End Sub

    Sub run_oledb_cmd(ByVal oledb_cmd As String, ByVal cn_str As String, ByRef err_msg As String)
        Try
            Using cn As New OleDbConnection(cn_str)
                cn.Open()
                Using cmd As New OleDbCommand(oledb_cmd, cn)
                    cmd.ExecuteNonQuery()
                End Using
                cn.Close()
            End Using
        Catch ex As OleDb.OleDbException
            err_msg = ex.Message.ToString()
        End Try
    End Sub


    Function get_dft_err_msg(ByVal src_sys_nm As String, ByVal file_path As String, ByVal src_platform As String) As String

        Dim err_msg As String = ""
        err_msg = ""
        err_msg = err_msg & "Unable to parse source query submitted for "
        If LCase(src_platform) = "excel" Then
            err_msg = err_msg & "file [" + file_path + "]. " + vbCrLf
            err_msg += "When querying an Excel Spreadsheet, ensure the sheet name is enclosed in square brackets " + vbCrLf
            err_msg = err_msg & " and that a '$' is appended to the end of the sheet name " + vbCrLf + "    e.g. 'SELECT * FROM [Sheet1$]' if you want to query data in Sheet1."
            err_msg += vbCrLf
            err_msg += "Below is the error readout: " + vbCrLf
            err_msg += "--------------------------:" + vbCrLf
            err_msg += df_task_build_err_msg
        ElseIf LCase(src_platform) = "access" Then
            err_msg += " file [" + file_path + "]. "
            err_msg += "Check your source query and validate the syntax before re-submitting. " + vbCrLf
            err_msg += "Below is the error readout: " + vbCrLf
            err_msg += "--------------------------:" + vbCrLf
            err_msg += df_task_build_err_msg
        Else
            err_msg += "system [" & src_sys_nm & "] during Data Flow Source Setup. "
            err_msg += "Check your source query and validate the syntax before re-submitting. " + vbCrLf
            err_msg += "Below is the error readout: " + vbCrLf
            err_msg += "--------------------------:" + vbCrLf
            err_msg += df_task_build_err_msg
        End If

        Return err_msg

    End Function


    'returns the destination system's equivalent data type based on what the ssis data type is
    'used for building dynamic destination tables and/or files
    Function data_type_translator(ssis_data_type As String, dest_sys_platform As String,
                                            Optional text_length As Integer = 0, Optional precision As Integer = 0, Optional scale As Integer = 0) As String

        Dim ret_str As String = ""

        Select Case UCase(dest_sys_platform)

            Case "MSSQL"

                'assign sql server data type
                Select Case UCase(ssis_data_type)
                    Case "DT_WSTR", "DT_NTEXT"
                        ret_str = "NVARCHAR"
                    Case "DT_STR", "DT_TEXT"
                        ret_str = "VARCHAR"
                    Case "DT_BOOL"
                        ret_str = "bit"
                    Case "DT_DBTIMESTAMP", "DT_DATE", "DT_DBTIMESTAMP2"
                        ret_str = "datetime2"
                    Case "DT_DBDATE"
                        ret_str = "date"
                    Case "DT_DBTIME", "DT_DBTIME2"
                        ret_str = "time"
                    Case "DT_I1", "DT_UI1"
                        ret_str = "tinyint"
                    Case "DT_I2", "DT_UI2"
                        ret_str = "smallint"
                    Case "DT_I4", "DT_UI4"
                        ret_str = "int"
                    'Case "DT_I1", "DT_I2", "DT_I4", "DT_UI1", "DT_UI1", "DT_UI2", "DT_UI4"
                    '    ret_str = "int"
                    Case "DT_I8", "DT_UI8"
                        ret_str = "bigint"
                    Case "DT_NUMERIC", "DT_DECIMAL"
                        ret_str = String.Format("DECIMAL({0},{1})", precision, scale)
                    Case "DT_CY"
                        ret_str = "money"
                    Case "DT_R4"
                        ret_str = "real"
                    Case "DT_R8"
                        ret_str = "float"
                    Case "DT_GUID"
                        ret_str = "uniqueidentifier"
                    Case "DT_BYTES"
                        ret_str = "varbinary"
                    Case "DT_IMAGE"
                        ret_str = "varbinary(max)"
                    Case Else
                        ret_str = "nvarchar(max)"

                End Select

                'add in length
                Select Case UCase(ssis_data_type)
                    Case "DT_NTEXT", "DT_TEXT"
                        ret_str = ret_str & "(MAX)"
                    Case "DT_WSTR", "DT_STR"
                        If text_length = -1 Then
                            ret_str = ret_str & "(MAX)"
                        Else
                            ret_str = ret_str & "(" & text_length & ")"
                        End If
                End Select

            Case "EXCEL"
                Select Case UCase(ssis_data_type)
                    Case "DT_STR", "DT_WSTR"
                        ret_str = "LongText"
                    Case "DT_NEXT", "DT_TEXT"
                        ret_str = "memo"
                    Case "DT_I1", "DT_I2", "DT_I4", "DT_I8", "DT_UI1", "DT_UI2", "DT_UI4", "DT_UI8", "DT_R4", "DT_R8", "DT_CY", "DT_NUMERIC"
                        ret_str = "Long"
                    Case "DT_DBTIMESTAMP", "DT_DATE", "DT_DBTIMESTAMP2", "DT_DBDATE"
                        ret_str = "DateTime"
                    Case Else
                        ret_str = "LongText"
                End Select

            Case "ACCESS"
                Select Case UCase(ssis_data_type)
                    Case "DT_TEXT", "DT_NTEXT", "DT_STR", "DT_WSTR"
                        ret_str = "LongText"
                    Case "DT_I1", "DT_I2", "DT_I4", "DT_I8", "DT_UI1", "DT_UI2", "DT_UI4", "DT_UI8", "DT_R4", "DT_R8", "DT_CY", "DT_NUMERIC"
                        ret_str = "Long"
                    Case "DT_DBTIMESTAMP", "DT_DATE", "DT_DBTIMESTAMP2", "DT_DBDATE"
                        ret_str = "DateTime"
                    Case Else
                        ret_str = "LongText"
                End Select


        End Select

        Return ret_str

    End Function

    'splits a string that can contain a text qualifier
    Friend Function custom_Split(
        ByVal expression As String,
        ByVal delimiter As String,
        ByVal qualifier As String,
        ByVal ignoreCase As Boolean) _
    As String()

        Dim qual_ind As Boolean = False
        Dim begin_ind As Integer = 0
        Dim arr_list As New System.Collections.ArrayList

        For char_pos As Integer = 0 To expression.Length - 1
            If Not qualifier Is Nothing And String.Compare(expression.Substring(char_pos, qualifier.Length), qualifier, ignoreCase) = 0 Then
                'trip bool flag
                qual_ind = Not qual_ind
                'if we have a text qualifier and a delimeter
            ElseIf Not qual_ind And Not delimiter Is Nothing AndAlso String.Compare(expression.Substring(char_pos, delimiter.Length), delimiter, ignoreCase) = 0 Then
                arr_list.Add(expression.Substring(begin_ind, char_pos - begin_ind))
                begin_ind = char_pos + 1
            End If
        Next

        If begin_ind < expression.Length Then
            arr_list.Add(expression.Substring(begin_ind, expression.Length - begin_ind))
        End If

        Dim res_list(arr_list.Count - 1) As String
        arr_list.CopyTo(res_list)
        Return res_list
    End Function

    Friend Function split_string(text As String, delim As String, qualifier As String, ignore_case As Boolean) As List(Of String)

        Dim res As New List(Of String)

        'if no delim, return the entire string
        If ignore_case Then
            If text.IndexOf(delim, 0, StringComparison.CurrentCultureIgnoreCase) = -1 Then
                res.Add(text)
                Return res
            End If
        Else
            If Not text.Contains(delim) Then
                res.Add(text)
                Return res
            End If
        End If

        'set qualifier to empty string if its nothing
        If qualifier Is Nothing Then
            qualifier = ""
        End If

        'otherwise, we are parsing
        Dim in_qual As Boolean = False
        Dim curr_field As String
        For i = 0 To text.Length - 1

            If Not String.IsNullOrWhiteSpace(qualifier) AndAlso (i + qualifier.Length <= text.Length) AndAlso String.Compare(text.Substring(i, qualifier.Length), qualifier, ignore_case) = 0 Then
                in_qual = Not in_qual
                i += qualifier.Length '- 1
            End If

            'if we are at the end
            If i >= text.Length Then
                Exit For

                'if we don't see a delimiter
            ElseIf (i + delim.Length <= text.Length) AndAlso String.Compare(text.Substring(i, delim.Length), delim, ignore_case) <> 0 Then
                curr_field += text.Substring(i, 1)
                'if we see a delimiter but are in a qualified text bracket
            ElseIf (i + delim.Length <= text.Length) AndAlso String.Compare(text.Substring(i, delim.Length), delim, ignore_case) = 0 And in_qual Then
                curr_field += text.Substring(i, 1)
                'if we hit the delim and are not in a qualified state, then add the field
            ElseIf (i + delim.Length <= text.Length) AndAlso String.Compare(text.Substring(i, delim.Length), delim, ignore_case) = 0 And Not in_qual Then
                res.Add(curr_field)
                curr_field = ""
                i += delim.Length - 1
            Else
                'just add the character
                curr_field += text.Substring(i, 1)
            End If

        Next

        'add last field
        If Not String.IsNullOrWhiteSpace(curr_field) Then
            res.Add(curr_field)
        End If

        Return res

    End Function

    Function get_pkg_annotation() As String


        Dim pkg_annotation As String = <![CDATA[<?xml version="1.0"?>
<!--This CDATA section contains the layout information of the package. The section includes information such as (x,y) coordinates, width, and height.-->
<!--If you manually edit this section and make a mistake, you can delete it. -->
<!--The package will still be able to load normally but the previous layout information will be lost and the designer will automatically re-arrange the elements on the design surface.-->
<Objects
  Version="8">
  <!--Each node below will contain properties that do not affect runtime behavior.-->
  <Package
    design-time-name="Package">
    <LayoutInfo>
      <GraphLayout
        Capacity="4" xmlns="clr-namespace:Microsoft.SqlServer.IntegrationServices.Designer.Model.Serialization;assembly=Microsoft.SqlServer.IntegrationServices.Graph" xmlns:mssge="clr-namespace:Microsoft.SqlServer.Graph.Extended;assembly=Microsoft.SqlServer.IntegrationServices.Graph" xmlns:av="http://schemas.microsoft.com/winfx/2006/xaml/presentation" xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">
        <NodeLayout
          Size="151,42"
          Id="Package\Data Flow Task"
          TopLeft="5.5,12.5" />
        <AnnotationLayout
          Text="COPYRIGHT © {COPYRIGHT_YEAR}, MD Data Technologies, LLC&#xA;"
          ParentId="Package"
          Size="376,58"
          Id="c27b8b40-99de-4301-a1e8-92d4f91eb911"
          TopLeft="247,72.5">
          <AnnotationLayout.FontInfo>
            <mssge:FontInfo
              Family="Tahoma"
              Size="14.25"
              Color="#FF000000"
              Weight="Bold">
              <mssge:FontInfo.TextDecorations>
                <av:TextDecorationCollection />
              </mssge:FontInfo.TextDecorations>
            </mssge:FontInfo>
          </AnnotationLayout.FontInfo>
        </AnnotationLayout>
        <AnnotationLayout
          Text="This package was autogenerated by EDIS with the following information:&#xD;&#xA;[1] Create Date: {PKG_CREATE_TS}&#xD;&#xA;[2] User: {USER_ID}&#xD;&#xA;[3] Machine Name: {MACHINE_NAME}"
          ParentId="Package"
          FontInfo="{x:Null}"
          Size="412,88"
          Id="9462da34-894c-492e-b6f5-a9310076f9e7"
          TopLeft="252,117" />
        <AnnotationLayout
          Text="For more information, visit www.sqletl.com&#xA;"
          ParentId="Package"
          Size="341,58"
          Id="7523358a-d4e4-4c9d-a6b6-7dd136114ab8"
          TopLeft="248.5,223">
          <AnnotationLayout.FontInfo>
            <mssge:FontInfo
              Family="Tahoma"
              Size="14.25"
              Color="#FF000000"
              Weight="Bold">
              <mssge:FontInfo.TextDecorations>
                <av:TextDecorationCollection />
              </mssge:FontInfo.TextDecorations>
            </mssge:FontInfo>
          </AnnotationLayout.FontInfo>
        </AnnotationLayout>
      </GraphLayout>
    </LayoutInfo>
  </Package>
</Objects>]]>.Value

        'pkg_annotation = Replace(pkg_annotation, "{PKG_ID}", pkg.ID.ToString)
        pkg_annotation = Replace(pkg_annotation, "{PKG_CREATE_TS}", Now.ToUniversalTime + " UTC")
        pkg_annotation = Replace(pkg_annotation, "{COPYRIGHT_YEAR}", Now.Year.ToString)
        pkg_annotation = Replace(pkg_annotation, "{MACHINE_NAME}", System.Environment.MachineName.ToString)
        pkg_annotation = Replace(pkg_annotation, "{USER_ID}", System.Environment.UserName.ToString)

        Return pkg_annotation

    End Function


End Module
