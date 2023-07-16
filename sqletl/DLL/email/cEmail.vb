
Imports System.Net.Mail
Imports System.IO
Imports Rebex.IO.Compression

Friend Class cEmail

    Friend Shared Sub send_email(ByRef err_msg As String)

        Try

            Dim port As String = EDIS.get_edis_pval("port")
            Dim enable_ssl As Boolean = CBool(EDIS.get_edis_pval("use_ssl"))
            Dim smtp_server As String = EDIS.get_edis_pval("smtp_host")
            Dim from_adrs As String = EDIS.get_edis_pval("from")
            Dim display_nm As String = EDIS.get_edis_pval("from_display_nm")
            Dim to_adrs As String = EDIS.get_edis_pval("to")
            Dim cc_adrs As String = EDIS.get_edis_pval("cc")
            Dim bcc_adrs As String = EDIS.get_edis_pval("bcc")
            Dim subject As String = EDIS.get_edis_pval("subject")
            Dim body As String = EDIS.get_edis_pval("body")
            Dim body_format As String = EDIS.get_edis_pval("body_format")
            Dim is_high_imp As Boolean = CBool(EDIS.get_edis_pval("is_high_pri"))
            Dim attachment_file_path As String = EDIS.get_edis_pval("file_attachment_path")
            Dim qry As String = EDIS.get_edis_pval("qry")
            Dim qry_results_file_path As String = EDIS.get_edis_pval("qry_attachment_path")
            Dim compress_qry_attachment As Boolean = CBool(EDIS.get_edis_pval("compress_qry_attachment"))
            Dim col_delim As String = EDIS.get_edis_pval("col_delim")

            Dim user_id As String = EDIS.get_edis_pval("svc_uid")
            Dim pwd As String = EDIS.get_edis_pval("svc_pwd")

            log_edis_event("info", "send email", "params assigned")

            Dim final_qry_attach_path As String = ""

            If Not String.IsNullOrWhiteSpace(qry) Then

                Dim ext As String = Right(qry_results_file_path, 4).ToLower

                Select Case ext
                    Case ".xls"
                        log_edis_event("info", "send email", "starting xls export")
                        cXL03.export_data(qry_results_file_path, EDIS._mssql_inst, qry, "data", True, False, True, False, err_msg)
                        If err_msg <> "" Then Exit Sub
                    Case "xlsx", "xlsm"
                        log_edis_event("info", "send email", "starting " + ext + " export")
                        cXL07.export_data(qry_results_file_path, EDIS._mssql_inst, qry, "data", True, False, True, False, err_msg)
                        If err_msg <> "" Then Exit Sub

                    Case ".txt", ".csv"

                        'for delimeter, if user supplied, use it, otherwise for csv, use comma, for txt, use tab
                        Dim delim As String
                        If Not String.IsNullOrWhiteSpace(col_delim) Then
                            delim = col_delim
                        Else
                            If ext = ".txt" Then
                                delim = "{TAB}"
                            ElseIf ext = ".csv" Then
                                delim = ","
                            End If
                        End If

                        Dim src_cn_str As String = String.Format("Provider = SQLOLEDB; Data Source = {0}; Integrated Security = SSPI", EDIS._mssql_inst)

                        log_edis_event("info", "send email", "starting data transfer flat file export")

                        cData_Transfer_Shell.run_data_transfer_db_to_ff_out(EDIS._mssql_inst, src_cn_str, "OLEDB", "", "MSSQL", qry, qry_results_file_path, delim,
                                                                            "{CR}{LF}", """", False, False, False, "", err_msg)
                        If err_msg <> "" Then Exit Sub
                End Select

                log_edis_event("info", "send email", "query results file created")

            End If



            If compress_qry_attachment Then

                Dim zip_path As String = Path.GetDirectoryName(qry_results_file_path)
                If Right(zip_path, 1) <> "\" Then zip_path += "\"

                Dim f_name As String = Path.GetFileNameWithoutExtension(qry_results_file_path)

                Dim zip_full_path As String = zip_path + f_name + ".zip"

                log_edis_event("info", "send email", "zip path [" + zip_full_path + "] created")

                If File.Exists(zip_full_path) Then
                    File.Delete(zip_full_path)
                End If

                Using zip As New ZipArchive(zip_full_path)
                    zip.Add(qry_results_file_path, Nothing, Rebex.IO.TraversalMode.MatchFilesShallow)
                End Using

                log_edis_event("info", "send email", "qry file [" + qry_results_file_path + "] zipped to path [" + zip_full_path + "]")

                If File.Exists(qry_results_file_path) Then
                    File.Delete(qry_results_file_path)
                    log_edis_event("info", "send email", "query results file [" + qry_results_file_path + "] deleted after zip")
                End If

                final_qry_attach_path = zip_full_path
            Else
                final_qry_attach_path = qry_results_file_path

            End If


            Dim email_msg As New MailMessage

            With email_msg

                If Not String.IsNullOrWhiteSpace(display_nm) Then
                    .From = New MailAddress(Trim(from_adrs), Trim(display_nm))
                Else
                    .From = New MailAddress(Trim(from_adrs))
                End If

                '.From = New MailAddress(Trim(from_adrs))
                .Sender = New MailAddress(Trim(from_adrs))


                If Not String.IsNullOrWhiteSpace(to_adrs) Then
                    Dim tos() As String = to_adrs.Split(";")
                    For Each t In tos
                        If Not String.IsNullOrWhiteSpace(t) Then
                            .To.Add(Trim(t.ToString()))
                        End If
                    Next
                End If

                log_edis_event("info", "send email", "to list added")

                If Not String.IsNullOrWhiteSpace(cc_adrs) Then
                    Dim ccs() As String = cc_adrs.Split(";")
                    For Each cc In ccs
                        If Not String.IsNullOrWhiteSpace(cc) Then
                            .CC.Add(Trim(cc.ToString()))
                        End If
                    Next
                End If

                log_edis_event("info", "send email", "cc list added")

                If Not String.IsNullOrWhiteSpace(bcc_adrs) Then
                    Dim bccs() As String = bcc_adrs.Split(";")
                    For Each bcc In bccs
                        If Not String.IsNullOrWhiteSpace(bcc) Then
                            .Bcc.Add(Trim(bcc.ToString()))
                        End If

                    Next
                End If

                log_edis_event("info", "send email", "bcc list added")

                .Subject = subject
                .Body = body
                If body_format.ToLower = "html" Then
                    .IsBodyHtml = True
                Else
                    .IsBodyHtml = False
                End If
                If is_high_imp Then
                    .Priority = MailPriority.High
                Else
                    .Priority = MailPriority.Normal
                End If

                If Not String.IsNullOrWhiteSpace(attachment_file_path) Then
                    Dim attachments() As String = attachment_file_path.Split(";")
                    For Each attach_file In attachments

                        If Not File.Exists(Trim(attach_file)) Then
                            err_msg = "Attachment [" + Trim(attach_file) + "] does not exist."
                            Exit Sub
                        End If

                        .Attachments.Add(New Attachment(Trim(attach_file)))

                        log_edis_event("info", "send email", "local attachment [" + attach_file + "] added")

                    Next
                End If

                'query attachment?
                If Not String.IsNullOrWhiteSpace(final_qry_attach_path) Then
                    .Attachments.Add(New Attachment(final_qry_attach_path))

                    log_edis_event("info", "send email", "query result attachment [" + final_qry_attach_path + "] added")

                End If

            End With

            Dim smtp As New SmtpClient

            If Not String.IsNullOrWhiteSpace(user_id) Then
                smtp.Credentials = New Net.NetworkCredential(user_id, pwd)

                log_edis_event("info", "send email", "email credentials loaded")
            End If

            smtp.Port = CInt(port)
            smtp.Host = smtp_server
            smtp.EnableSsl = enable_ssl


            log_edis_event("info", "send email", "sending email")

            smtp.Send(email_msg)

            smtp.Dispose()
            smtp = Nothing

            email_msg.Dispose()
            email_msg = Nothing

            log_edis_event("info", "send email", "email sent")

            'clean up
            If Not String.IsNullOrWhiteSpace(final_qry_attach_path) Then
                Dim counter As Integer = 0
retry_delete:
                Try
                    File.Delete(final_qry_attach_path)
                    log_edis_event("info", "send email", "query results file deleted")
                Catch ex As Exception

                    If ex.Message.ToString.ToLower Like "*the process cannot access the file*because it is being used by another process*" Then
                        'after 20...kill it and raise error
                        If counter >= 20 Then
                            err_msg = ex.Message.ToString()
                            Exit Sub
                        End If
                        Threading.Thread.Sleep(1000)
                        counter += 1
                        GoTo retry_delete
                    Else
                        err_msg = ex.Message.ToString()
                        Exit Sub
                    End If

                End Try


            End If

            log_edis_event("info", "send email", "email process complete")

        Catch ex As SmtpException

            err_msg = ex.GetBaseException().ToString()

            'err_msg = ex.Message.ToString()

        Catch ex As Exception
            err_msg = ex.GetBaseException().ToString()
        End Try
    End Sub

End Class
