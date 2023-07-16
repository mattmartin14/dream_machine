
Imports System.Security.Cryptography
Imports System.Text

Class cSecurity

    Shared Function is_pro_user(ByVal mssql_inst As String, ByVal install_id As String) As Boolean



        Dim is_pro As Boolean = False

        Dim sql_inst_nm As String = Replace(mssql_inst, "\", "_")

        Dim pro_hash As String = Left(get_hash("pro", sql_inst_nm), 32)

        If Trim(Left(install_id, 32)) = pro_hash Then is_pro = True

        Return is_pro

    End Function

    Friend Shared Function check_feature() As String

        Dim res As String = ""

        Dim primary_task As String = EDIS.get_edis_pval("primary_task").ToLower

        Select Case primary_task
            Case "data_transfer_task"

                Dim src_cn_type As String = EDIS.get_edis_pval("src_cn_type")
                Dim dest_cn_type As String = EDIS.get_edis_pval("dest_cn_type")

                If InStr(UCase(src_cn_type), "ADO.NET") Then
                    res = "ADO.NET source is a pro-only feature. "
                    GoTo finish
                End If

                If InStr(UCase(dest_cn_type), "ADO.NET") Then
                    res = "ADO.NET destination is a pro-only feature. "
                    GoTo finish
                End If

                If InStr(UCase(src_cn_type), "RAW") Then
                    res = "RAW source is a pro-only feature. "
                    GoTo finish
                End If

                If InStr(UCase(dest_cn_type), "RAW") Then
                    res = "RAW destination is a pro-only feature. "
                    GoTo finish
                End If

                If InStr(UCase(dest_cn_type), "FLATFILE") Then
                    res = "FLATFILE destination is a pro-only feature. "
                    GoTo finish
                End If

                'pro only: 
                '   Raw as source/dest
                '   ado.net as source/dest
                '   flat file destination
                '   proxy account -- enforce sql side?


            Case "sql_cmd_task"
                'all supported
            Case "ftp_task"
                'pro only
                '   sftp and ftps

                Dim use_ssl As Boolean = CBool(EDIS.get_edis_pval("use_ssl"))
                Dim use_ssh As Boolean = CBool(EDIS.get_edis_pval("use_ssh"))

                If use_ssl Then
                    res = "FTP SSL (FTP-S) support is a pro-only feature. "
                    GoTo finish
                End If

                If use_ssh Then
                    res = "FTP SSH (Secure FTP/SFTP) support is a pro-only feature. "
                    GoTo finish
                End If

            Case "file_task"
                res = "" 'all file tasks are free
            Case "zip_task"
                res = "Zip Task is a pro-only feature. "
                GoTo finish
            Case "process_task"
                'all supported
            Case "web_task"
                res = "Web Task is a pro-only feature. "
                GoTo finish
            Case "sharepoint_task"
                res = "SharePoint Task is a pro-only feature. "
                GoTo finish
            Case "excel_task"
                'pro_only
                '   over 1k rows 'enforce on procs
                '   bold/autofilter,autofit
                '   clear/rename/add worksheets

                Dim sub_task As String = EDIS.get_edis_pval("sub_task").ToLower

                If sub_task = "clear_worksheet" Then
                    res = "Excel clear-sheet task is a pro-only feature. "
                    GoTo finish
                ElseIf sub_task = "rename_worksheet" Then
                    res = "Excel rename-sheet task is a pro-only feature. "
                    GoTo finish
                ElseIf sub_task = "add_worksheet" Then
                    res = "Excel add-sheet task is a pro-only feature. "
                    GoTo finish
                ElseIf sub_task = "export_data" Then
                    Dim bold_headers As Boolean = CBool(EDIS.get_edis_pval("bold_headers"))
                    Dim apply_autofilter As Boolean = CBool(EDIS.get_edis_pval("use_autofilter"))
                    Dim autofit_cols As Boolean = CBool(EDIS.get_edis_pval("autofit_cols"))

                    If bold_headers Then
                        res = "Excel Export: bold-headers is a pro-only feature. "
                        GoTo finish
                    End If

                    If apply_autofilter Then
                        res = "Excel Export: Autofilter is a pro-only feature. "
                        GoTo finish
                    End If

                    If autofit_cols Then
                        res = "Excel Export: Autofitting columns is a pro-only feature. "
                        GoTo finish
                    End If

                End If


            Case "flatfile_task"
                'no idea yet
            Case "bigquery_task"
                res = "BigQuery Task is a pro-only feature. "
                GoTo finish
            Case "powershell_task"
                'all supported
        End Select

finish:

        Dim go_here_to_purchase As String = "Please visit www.sqletl.com to purchase an EDIS Pro license."

        If Not String.IsNullOrWhiteSpace(res) Then
            res += go_here_to_purchase
        End If

        Return res

    End Function

    Private Shared Function get_hash(ByVal version As String, ByVal sql_srvr_name As String) As String

        Dim res As String = ""
        Dim hash_string As String = sql_srvr_name + "_XL$#NCAG$%*&_FULL"

        Using MDHash As MD5 = MD5.Create()

            Dim data As Byte() = MDHash.ComputeHash(Encoding.UTF8.GetBytes(hash_string))
            Dim sBuilder As New StringBuilder()

            Dim i As Integer
            For i = 0 To data.Length - 1
                sBuilder.Append(data(i).ToString("x2"))
            Next i

            res = sBuilder.ToString()

        End Using

        Return res

    End Function

End Class
