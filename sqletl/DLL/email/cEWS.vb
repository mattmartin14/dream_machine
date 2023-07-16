
Imports msEws = Microsoft.Exchange.WebServices
Imports Microsoft.Exchange.WebServices
Imports System.Data.SqlClient
Imports System.Text
Imports System.IO
Friend Class cEWS

    Friend Shared Sub main(ByRef err_msg As String)

        Try
            Dim exchng_vsn As String = EDIS.get_edis_pval("exchange_version")
            Dim user As String = EDIS.get_edis_pval("svc_uid")
            Dim pass As String = EDIS.get_edis_pval("svc_pwd")
            Dim domain As String = EDIS.get_edis_pval("domain")
            Dim account As String = EDIS.get_edis_pval("email_acct")
            Dim o365url As String = EDIS.get_edis_pval("office_365_url")


            Dim ews As New msEws.Data.ExchangeService
            _connect_to_ews(user, pass, domain, account, exchng_vsn, o365url, ews, err_msg)
            If err_msg <> "" Then
                Exit Sub
            End If

            Select Case EDIS.get_edis_pval("sub_task").ToLower
                Case "send_email"
                    Dim sender As String = EDIS.get_edis_pval("sender")
                    Dim subject As String = EDIS.get_edis_pval("subject")
                    Dim body As String = EDIS.get_edis_pval("body")
                    Dim to_list As String = EDIS.get_edis_pval("to_list")
                    Dim cc_list As String = EDIS.get_edis_pval("cc_list")
                    Dim bcc_list As String = EDIS.get_edis_pval("bcc_list")

                    send_email(ews, subject, body, to_list, cc_list, bcc_list, err_msg)

                Case "import_messages"
                    Dim folder_path As String = EDIS.get_edis_pval("folder_path")
                    Dim body_format As String = EDIS.get_edis_pval("body_format")
                    Dim include_attachment_content As Boolean = CBool(EDIS.get_edis_pval("include_attachment_content"))
                    Dim target_db As String = EDIS.get_edis_pval("target_db")
                    Dim target_schema As String = EDIS.get_edis_pval("target_schema")
                    Dim target_tbl As String = EDIS.get_edis_pval("target_tbl")
                    Dim subject_filter As String = EDIS.get_edis_pval("subject_filter")
                    Dim msg_start_dt As String = EDIS.get_edis_pval("msg_start_dt")
                    Dim msg_end_dt As String = EDIS.get_edis_pval("msg_end_dt")
                    Dim attachment_nm_filter As String = EDIS.get_edis_pval("attachment_nm_filter")


                    import_email_content(ews, folder_path, body_format,
                                         include_attachment_content, EDIS._mssql_inst, target_db, target_schema, target_tbl, err_msg, subject_filter,
                                         msg_start_dt, msg_end_dt, attachment_nm_filter)
                Case "move_messages"
                    Dim src_folder_path As String = EDIS.get_edis_pval("src_folder_path")
                    Dim tgt_folder_path As String = EDIS.get_edis_pval("tgt_folder_path")
                    Dim msg_list_str As String = EDIS.get_edis_pval("msg_id_list")

                    Dim msg_id_list As New List(Of String)

                    msg_id_list = msg_list_str.Split(";").ToList()

                    move_messages(ews, src_folder_path, tgt_folder_path, msg_id_list, err_msg)

                Case "delete_messages"
                    Dim msg_list_str As String = EDIS.get_edis_pval("msg_id_list")
                    Dim msg_id_list As New List(Of String)
                    msg_id_list = msg_list_str.Split(";").ToList()

                    delete_messages(ews, msg_id_list, err_msg)

                Case "create_folder"
                    Dim folder_nm As String = EDIS.get_edis_pval("folder_nm")
                    Dim parent_folder_path As String = EDIS.get_edis_pval("parent_folder_path")

                    create_folder(ews, folder_nm, parent_folder_path, err_msg)
                Case "delete_folder"
                    Dim folder_nm As String = EDIS.get_edis_pval("folder_nm")
                    Dim parent_folder_path As String = EDIS.get_edis_pval("parent_folder_path")

                    delete_folder(ews, folder_nm, parent_folder_path, err_msg)

                Case "create_calendar_appt"
                    Dim start_ts As String = EDIS.get_edis_pval("start_ts")
                    Dim end_ts As String = EDIS.get_edis_pval("end_ts")
                    Dim subject As String = EDIS.get_edis_pval("subject")
                    Dim body As String = EDIS.get_edis_pval("body")
                    Dim location As String = EDIS.get_edis_pval("location")
                    Dim invite_list As String = EDIS.get_edis_pval("invite_list")
                    Dim category As String = EDIS.get_edis_pval("category")

                    create_calendar_appt(ews, subject, body, location, category, start_ts, end_ts, invite_list, err_msg)

            End Select

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub

    'Current status:
    ' [1] Have import messages...pretty good
    ' [2] have a move message
    '       - the source proc in sql needs to cast the email id to varchar(512) so its in string format
    ' [3] have a delete message
    '       - functions same as move message...you have to specify the message id
    ' [4] create folder
    ' [5] delete folder
    ' [6] create calendar appointment
    '------------------------------------------
    'outstanding
    ' send email...works, need to integrate excel export/zip/etc
    ' create folder/delete folder

    Friend Shared Sub create_calendar_appt(ews As Data.ExchangeService, subject As String, body As String, location As String, category As String, start_ts As String, end_ts As String, invite_list As String, ByRef err_msg As String)
        Try
            Dim appt As Data.Appointment = New Data.Appointment(ews)
            appt.Subject = subject
            appt.Body = body
            appt.Start = Convert.ToDateTime(start_ts)
            appt.End = Convert.ToDateTime(end_ts)
            If Not String.IsNullOrWhiteSpace(location) Then
                appt.Location = location
            End If

            If Not String.IsNullOrWhiteSpace(category) Then
                appt.Categories.Add(category)
            End If


            If Not String.IsNullOrWhiteSpace(invite_list) Then

                Dim peeps As List(Of String) = invite_list.Split(";").ToList()
                'peeps = invite_list.Split(";").ToList
                For Each peep In peeps
                    appt.RequiredAttendees.Add(peep)
                Next

                appt.Save(Data.SendInvitationsMode.SendOnlyToAll)
            Else
                appt.Save(Data.SendInvitationsMode.SendToNone)
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

    Friend Shared Sub create_folder(ews As Data.ExchangeService, folder_nm As String, parent_folder_path As String, ByRef err_msg As String)

        Try

            Dim parent_folder = _get_ews_folder(ews, parent_folder_path, err_msg)

            Dim folder As Data.Folder = New Data.Folder(ews)
            folder.DisplayName = folder_nm

            folder.Save(parent_folder.Id)

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Friend Shared Sub delete_folder(ews As Data.ExchangeService, folder_nm As String, parent_folder_path As String, ByRef err_msg As String)

        Try

            Dim tgt_folder_path As String
            If Right(parent_folder_path, 1) <> "/" Then
                parent_folder_path += "/"
            End If

            tgt_folder_path = parent_folder_path + folder_nm

            Dim folder_to_delete = _get_ews_folder(ews, tgt_folder_path, err_msg)

            folder_to_delete.Delete(Data.DeleteMode.MoveToDeletedItems)

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try
    End Sub

    Friend Shared Sub send_email(ews As Data.ExchangeService, subject As String, body As String,
                                 to_list As String, cc_list As String, bcc_list As String,
                                 ByRef err_msg As String
                                 )
        Try
            Dim emsg As New msEws.Data.EmailMessage(ews)

            emsg.Subject = subject
            emsg.Body = body

            'emsg.Attachments.AddFileAttachment("C:\temp\test.xlsx")

            emsg.ToRecipients.Add(to_list)

            'emsg.send just sends
            'emsg.sendandsavecopy shows in the sent folder
            emsg.SendAndSaveCopy()

            ews = Nothing
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub

    Friend Shared Sub move_messages(ews As Data.ExchangeService,
                                           src_folder_path As String,
                                           tgt_folder_path As String,
                                           msg_id_list As List(Of String),
                                           ByRef err_msg As String
                                   )

        Try

            Dim src_folder = _get_ews_folder(ews, src_folder_path, err_msg)
            If src_folder Is Nothing Then

                Dim avail_list As String = String.Join("; ", _get_folder_list(ews))

                err_msg = "Unable to locate Exchange Folder [" + src_folder_path + "]." + vbCrLf
                err_msg += "Available folders are: " + vbCrLf
                err_msg += "------------------" + vbCrLf
                err_msg += avail_list

                Exit Sub
            End If

            Dim tgt_folder = _get_ews_folder(ews, tgt_folder_path, err_msg)
            If tgt_folder Is Nothing Then

                Dim avail_list As String = String.Join("; ", _get_folder_list(ews))

                err_msg = "Unable to locate Exchange Folder [" + tgt_folder_path + "]." + vbCrLf
                err_msg += "Available folders are: " + vbCrLf
                err_msg += "------------------" + vbCrLf
                err_msg += avail_list

                Exit Sub
            End If

            Dim batch_size As Integer = 25
            Dim curr_batch As Integer = 0

            Dim msgs_batch As New List(Of Data.ItemId)

            'move in batches set to the batch_size
            For Each itm In msg_id_list
                msgs_batch.Add(itm.ToString)

                curr_batch += 1

                If curr_batch >= batch_size Then
                    Dim svc_resp As Data.ServiceResponseCollection(Of Data.MoveCopyItemResponse)

                    svc_resp = ews.MoveItems(msgs_batch, tgt_folder.Id)

                    'gather any errors
                    If svc_resp.OverallResult = Data.ServiceResult.Error Then
                        For Each rsp In svc_resp
                            err_msg += rsp.ErrorMessage + vbCrLf
                        Next
                        Exit Sub
                    End If

                    curr_batch = 0
                    msgs_batch.Clear()
                End If

            Next

            If curr_batch >= 1 Then
                Dim svc_resp As Data.ServiceResponseCollection(Of Data.MoveCopyItemResponse)

                svc_resp = ews.MoveItems(msgs_batch, tgt_folder.Id)

                'gather any errors
                If svc_resp.OverallResult = Data.ServiceResult.Error Then
                    For Each rsp In svc_resp
                        err_msg += rsp.ErrorMessage + vbCrLf
                    Next
                    Exit Sub
                End If
            End If


            'this does row by row
            ''process list
            'For Each itm In msg_id_list
            '    Dim curr_msg = _get_email_msg_by_id(ews, itm, err_msg)
            '    curr_msg.Move(tgt_folder.Id)
            'Next

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try


    End Sub

    Friend Shared Sub delete_messages(ews As Data.ExchangeService,
                                           msg_id_list As List(Of String),
                                           ByRef err_msg As String
                                   )

        Try

            'Dim trash As Data.Folder = _get_ews_folder(ews, "deleted items", err_msg)
            ''update this to process in batch 25 emails at once...

            Dim batch_size As Integer = 25
            Dim curr_batch_size As Integer = 0

            Dim msgs_batch As New List(Of Data.ItemId)

            For Each itm In msg_id_list



                msgs_batch.Add(itm.ToString)

                curr_batch_size += 1
                If curr_batch_size >= batch_size Then
                    Dim svc_resp As Data.ServiceResponseCollection(Of Data.ServiceResponse)

                    svc_resp = ews.DeleteItems(msgs_batch, Data.DeleteMode.MoveToDeletedItems, Nothing, Data.AffectedTaskOccurrence.AllOccurrences)

                    'gather any errors
                    If svc_resp.OverallResult = Data.ServiceResult.Error Then
                        For Each rsp In svc_resp
                            err_msg += rsp.ErrorMessage + vbCrLf
                        Next

                        Exit Sub
                    End If

                    curr_batch_size = 0
                    msgs_batch.Clear()

                End If

            Next

            'send last batch
            If curr_batch_size >= 1 Then
                Dim svc_resp As Data.ServiceResponseCollection(Of Data.ServiceResponse)

                svc_resp = ews.DeleteItems(msgs_batch, Data.DeleteMode.MoveToDeletedItems, Nothing, Data.AffectedTaskOccurrence.AllOccurrences)

                'gather any errors
                If svc_resp.OverallResult = Data.ServiceResult.Error Then
                    For Each rsp In svc_resp
                        err_msg += rsp.ErrorMessage + vbCrLf
                    Next

                    Exit Sub
                End If
            End If



            ''process list
            'For Each itm In msg_id_list
            '    Dim curr_msg = _get_email_msg_by_id(ews, itm, err_msg)
            '    'move it to the trash instead of a soft delete...users cant get to the soft delete folder easily
            '    curr_msg.Delete(Data.DeleteMode.MoveToDeletedItems)
            '    'curr_msg.Move(trash.Id)
            'Next

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try


    End Sub


    Friend Shared Sub import_email_content(ews As Data.ExchangeService,
                                           folder_path As String,
                                           body_format As String,
                                           include_attachment_content As Boolean,
                                           mssql_inst As String,
                                           target_db As String,
                                           target_schema As String,
                                           target_tbl As String,
                                           ByRef err_msg As String,
                                           subject_filter As String,
                                           msg_start_dt As String,
                                           msg_end_dt As String,
                                           attach_file_filter As String
                                 )

        Try

            log_edis_event("EWS Task", "Import Messages", "start")

            Dim batch_size As Integer = 100
            Dim attach_buffer As Integer = 1024 * 4 '4 mb
            Dim curr_attach_buffer As Integer = 0
            Dim curr_batch_cnt As Integer = 0

            Dim inbox = _get_ews_folder(ews, folder_path, err_msg)

            log_edis_event("EWS Task", "Import Messages", "target folder set")



            If inbox Is Nothing Then

                Dim avail_list As String = String.Join("; ", _get_folder_list(ews))

                err_msg = "Unable to locate Exchange Folder [" + folder_path + "]." + vbCrLf
                err_msg += "Available folders are: " + vbCrLf
                err_msg += "------------------" + vbCrLf
                err_msg += avail_list



                Exit Sub
            End If

            Dim vItems As Data.ItemView = New Data.ItemView(100)
            vItems.PropertySet = New Data.PropertySet(Data.PropertySet.IdOnly)

            Dim pset As Data.PropertySet = New Data.PropertySet()

            pset.Add(Data.ItemSchema.Body)
            pset.Add(Data.ItemSchema.Subject)
            pset.Add(Data.ItemSchema.DisplayTo)
            pset.Add(Data.ItemSchema.DisplayCc)
            pset.Add(Data.EmailMessageSchema.From)
            pset.Add(Data.EmailMessageSchema.ToRecipients)
            pset.Add(Data.EmailMessageSchema.CcRecipients)
            pset.Add(Data.EmailMessageSchema.Id)
            pset.Add(Data.EmailMessageSchema.HasAttachments)
            If include_attachment_content Then
                pset.Add(Data.EmailMessageSchema.Attachments)
            End If

            pset.Add(Data.EmailMessageSchema.DateTimeReceived)
            pset.Add(Data.EmailMessageSchema.DateTimeSent)
            pset.Add(Data.EmailMessageSchema.Importance)

            If body_format.ToLower = "text" Then
                pset.RequestedBodyType = Data.BodyType.Text
            End If


            log_edis_event("EWS Task", "Import Messages", "mail properties set")


            Dim findItems

            Dim sfc As Data.SearchFilter.SearchFilterCollection = New Data.SearchFilter.SearchFilterCollection

            If Not String.IsNullOrWhiteSpace(subject_filter) Then
                Dim ews_sub_filter As Data.SearchFilter = New Data.SearchFilter.ContainsSubstring(Data.ItemSchema.Subject, subject_filter)
                sfc.Add(ews_sub_filter)
            End If

            If Not String.IsNullOrWhiteSpace(msg_start_dt) Then

                Dim sYear As Integer = DatePart(DateInterval.Year, CDate(msg_start_dt))
                Dim sMonth As Integer = DatePart(DateInterval.Month, CDate(msg_start_dt))
                Dim sDay As Integer = DatePart(DateInterval.Day, CDate(msg_start_dt))

                Dim start_date_fmt As New DateTime(sYear, sMonth, sDay)

                Dim start_dt_anchor As Data.SearchFilter = New Data.SearchFilter.IsGreaterThanOrEqualTo(Data.ItemSchema.DateTimeReceived, start_date_fmt)
                sfc.Add(start_dt_anchor)
            End If

            If Not String.IsNullOrWhiteSpace(msg_end_dt) Then

                Dim eYear As Integer = DatePart(DateInterval.Year, CDate(msg_end_dt))
                Dim eMonth As Integer = DatePart(DateInterval.Month, CDate(msg_end_dt))
                Dim eDay As Integer = DatePart(DateInterval.Day, CDate(msg_end_dt))

                Dim end_date_fmt As New DateTime(eYear, eMonth, eDay)

                Dim end_dt_anchor As Data.SearchFilter = New Data.SearchFilter.IsLessThanOrEqualTo(Data.ItemSchema.DateTimeReceived, end_date_fmt)
                sfc.Add(end_dt_anchor)
            End If

            Dim cnt As Integer = 0

            Dim dt As New DataTable
            'dt.Columns.Add("email_id", GetType(Byte()))
            dt.Columns.Add("email_id", GetType(String))
            dt.Columns.Add("folder_path", GetType(String))
            dt.Columns.Add("date_sent", GetType(DateTime))
            dt.Columns.Add("date_received", GetType(DateTime))
            dt.Columns.Add("subject", GetType(String))
            dt.Columns.Add("body", GetType(String))
            dt.Columns.Add("to_recipients", GetType(String))
            dt.Columns.Add("cc_recipients", GetType(String))
            dt.Columns.Add("has_attachments", GetType(Boolean))
            dt.Columns.Add("attachments", GetType(String))
            dt.Columns.Add("attachment_cnt", GetType(Integer))
            dt.Columns.Add("is_reply", GetType(Boolean))
            dt.Columns.Add("is_forward", GetType(Boolean))
            dt.Columns.Add("importance", GetType(String))


            Do

                If sfc.Count >= 1 Then
                    findItems = inbox.FindItems(sfc, vItems)
                Else
                    findItems = inbox.FindItems(vItems)
                End If



                If findItems.Items.Count > 0 Then

                    ews.LoadPropertiesForItems(findItems.Items, pset)

                    For Each itm As Data.EmailMessage In findItems.Items

                        Dim subject As String = Nothing
                        If itm.Subject IsNot Nothing Then
                            subject = itm.Subject
                        End If

                        Dim dr As DataRow = dt.NewRow

                        'Dim id_hash As Byte() = Encoding.ASCII.GetBytes(itm.Id.ToString())

                        'dr.Item("email_id") = id_hash
                        dr.Item("email_id") = itm.Id.ToString()
                        dr.Item("folder_path") = folder_path

                        If subject IsNot Nothing Then
                            dr.Item("subject") = subject
                        Else
                            dr.Item("subject") = DBNull.Value
                        End If


                        dr.Item("body") = itm.Body.ToString()
                        Dim tos As String = String.Join(";", itm.ToRecipients.Select(Function(x) x.Address).ToList())
                        If String.IsNullOrWhiteSpace(tos) Then
                            dr.Item("to_recipients") = DBNull.Value
                        Else
                            dr.Item("to_recipients") = tos
                        End If

                        Dim ccs As String = String.Join(";", itm.CcRecipients.Select(Function(x) x.Address).ToList())
                        If String.IsNullOrWhiteSpace(ccs) Then
                            dr.Item("cc_recipients") = DBNull.Value
                        Else
                            dr.Item("cc_recipients") = ccs
                        End If

                        dr.Item("has_attachments") = itm.HasAttachments
                        dr.Item("date_received") = itm.DateTimeReceived
                        dr.Item("date_sent") = itm.DateTimeSent
                        dr.Item("importance") = itm.Importance
                        If Left(subject, 3) = "RE:" Then
                            dr.Item("is_reply") = True
                        Else
                            dr.Item("is_reply") = False
                        End If

                        If Left(subject, 3) = "FW:" Then
                            dr.Item("is_forward") = True
                        Else
                            dr.Item("is_forward") = False
                        End If

                        'grab attachments if they exist
                        If (itm.HasAttachments And include_attachment_content) Then

                            Dim attach_counter As Integer = 0

                            If String.IsNullOrWhiteSpace(attach_file_filter) Then
                                attach_file_filter = "*"
                            End If

                            Dim xdoc As XDocument = New XDocument(New XElement("ATTACHMENTS"))

                            Dim root = xdoc.Root

                            'Console.WriteLine("attachments found")
                            For Each att In itm.Attachments

                                If TypeOf att Is Data.FileAttachment Then
                                    Dim fatt As Data.FileAttachment = DirectCast(att, Data.FileAttachment)

                                    If fatt.Name Like attach_file_filter Then
                                        fatt.Load()

                                        Dim attach_content As Byte() = fatt.Content

                                        curr_attach_buffer += attach_content.Length

                                        Dim attach_content_str As String = Convert.ToBase64String(attach_content)


                                        root.Add(New XElement("ATTACHMENT",
                                                    New XAttribute("ATTACHMENT_NAME", fatt.Name),
                                                    New XAttribute("ATTACHMENT_CONTENT", attach_content_str)
                                        ))


                                        attach_counter += 1
                                    End If


                                End If
                            Next

                            If attach_counter >= 1 Then
                                dr.Item("attachments") = xdoc.ToString()
                                dr.Item("attachment_cnt") = attach_counter
                            Else
                                dr.Item("attachments") = DBNull.Value
                            End If

                            'data = Nothing

                            xdoc = Nothing


                        End If

                        dt.Rows.Add(dr)

                        curr_batch_cnt += 1

                        If (curr_batch_cnt >= batch_size Or curr_attach_buffer >= attach_buffer) Then
                            bulk_insert(mssql_inst, target_db, target_schema, target_tbl, dt, batch_size, False, Nothing, Nothing, err_msg)
                            If err_msg <> "" Then Exit Sub
                            dt.Clear()
                            curr_batch_cnt = 0
                            curr_attach_buffer = 0
                            'GC.Collect()
                        End If

                    Next
                End If


                vItems.Offset += findItems.Items.Count



            Loop While findItems.MoreAvailable()

            'final batch
            If curr_batch_cnt >= 1 Then
                bulk_insert(mssql_inst, target_db, target_schema, target_tbl, dt, batch_size, False, Nothing, Nothing, err_msg)
                If err_msg <> "" Then Exit Sub
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try


    End Sub

    Friend Shared Sub bulk_insert(ByVal mssql_inst As String, ByVal tgt_db As String, tgt_schema As String, tgt_tbl As String,
                                  ByVal dt As DataTable,
                                  ByVal batch_size As String, ByVal load_on_ordinal As Boolean,
                                  sql_user_id As String, sql_password As String, ByRef err_msg As String)

        Dim err_msg_prefix As String = "Bulk Insert Error: "
        Try
            Dim cn_str As String

            'if not supplying a user id, then it uses integrated security
            If String.IsNullOrWhiteSpace(sql_user_id) Then
                cn_str = String.Format("Server = {0}; Integrated Security = True; Database = {1}", mssql_inst, tgt_db)
                'log_edis_event("info", "Bulk Insert", "connection set using integrated security")
            Else
                cn_str = String.Format("Server = {0}; User ID = {1}; Password = {2}; Database = {3}", mssql_inst, sql_user_id, sql_password, tgt_db)
                'log_edis_event("info", "Bulk Insert", "connection set using SQL Server Authentication")
            End If

            Using cn As New SqlClient.SqlConnection(cn_str)
                cn.Open()

                Using bc As New SqlBulkCopy(cn_str, SqlBulkCopyOptions.TableLock)
                    bc.BatchSize = batch_size
                    bc.BulkCopyTimeout = 0
                    bc.DestinationTableName = "[" + tgt_schema + "].[" + tgt_tbl + "]"
                    'Bulk copy default is ordinal
                    If Not load_on_ordinal Then
                        For Each col In dt.Columns
                            Dim mp As New SqlClient.SqlBulkCopyColumnMapping()
                            mp.SourceColumn = col.ToString
                            mp.DestinationColumn = col.ToString
                            bc.ColumnMappings.Add(mp)
                        Next
                    End If

                    bc.WriteToServer(dt)

                End Using

            End Using



        Catch ex As Exception

            err_msg = err_msg_prefix + ex.Message.ToString()

        End Try

    End Sub


    Private Shared Function _get_email_msg_by_id(ews As Data.ExchangeService, msg_id As String, ByRef err_msg As String) As Data.EmailMessage

        Dim msg As Data.EmailMessage

        Try
            msg = Data.EmailMessage.Bind(ews, New Data.ItemId(msg_id))
            If msg Is Nothing Then
                err_msg = "Message not found"
                Return Nothing
            End If
            Return msg
        Catch ex As Exception
            Return Nothing
        End Try



    End Function

    Private Shared Sub _connect_to_ews(user As String, pass As String, domain As String,
                                      user_email_address As String,
                                      exchng_vsn As String,
                                      o365url As String,
                                      ByRef ews As Data.ExchangeService,
                                      ByRef err_msg As String)

        Try

            log_edis_event("EWS Task", "create connection", "user [" + user + "]")
            If Not String.IsNullOrWhiteSpace(domain) Then
                log_edis_event("EWS Task", "create connection", "domain [" + domain + "]")
            End If

            log_edis_event("EWS Task", "create connection", "acct [" + user_email_address + "]")


            'debug 
            'log_edis_event("EWS Task", "create connection", String.Format("User {0}, domain {1}, acct {2}", user, domain, user_email_address))

            'DEBUG ONLY >>>> COMMENT THIS OUT
            'log_edis_event("EWS Task", "create connection", "password [" + pass + "]")

            ews = New msEws.Data.ExchangeService(_get_exchange_vsn(exchng_vsn))

            If Not String.IsNullOrWhiteSpace(domain) Then
                'ews.Credentials = New Net.NetworkCredential(user, pass, domain)
                ews.Credentials = New Microsoft.Exchange.WebServices.Data.WebCredentials(user, pass, domain)
            Else
                'ews.Credentials = New Net.NetworkCredential(user, pass)
                ews.Credentials = New Microsoft.Exchange.WebServices.Data.WebCredentials(user, pass)
            End If

            'ews.Credentials = New Microsoft.Exchange.WebServices.Data.WebCredentials("", "", "")

            Console.WriteLine("ews credentials set")

            If Not String.IsNullOrWhiteSpace(o365url) Then
                log_edis_event("EWS Task", "setting ews connection", "loading ews o365 url [" + o365url + "]")
                ews.Url = New Uri(o365url)
            Else
                ews.AutodiscoverUrl(user_email_address)
            End If

            log_edis_event("EWS Task", "creating EWS Connection", "connection loaded")

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try


    End Sub

    Private Shared Function _get_folder_list(ews As msEws.Data.ExchangeService) As List(Of String)
        Dim ewsFolderView As New Data.FolderView(9999)
        ewsFolderView.Traversal = Data.FolderTraversal.Deep
        Dim ib = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.Inbox)
        Dim fFoldersRes = ib.FindFolders(ewsFolderView)

        'Dim l1 As New List(Of String)
        Return fFoldersRes.Select(Function(x) x.DisplayName).ToList()

    End Function

    Private Shared Function _get_ews_folder(ByVal ews As msEws.Data.ExchangeService, ByVal folder_path As String, ByRef err_msg As String) As msEws.Data.Folder

        Try

            'split the folder path
            Dim folders As String() = folder_path.Split(New String() {"/"}, StringSplitOptions.RemoveEmptyEntries)

            Dim root_folder As Data.Folder

            Select Case folders(0).ToString.ToLower()
                Case "inbox"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.Inbox)
                Case "sent items"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.SentItems)
                Case "drafts"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.Drafts)
                Case "outbox"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.Outbox)
                Case "junk"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.JunkEmail)
                Case "deleted items"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.DeletedItems)
                Case "outbox"
                    root_folder = msEws.Data.Folder.Bind(ews, Microsoft.Exchange.WebServices.Data.WellKnownFolderName.Outbox)
                Case Else
                    err_msg = "Root Folder [" + folders(0).ToString + "] does not exist. The Root folder needs to be one of the following"
                    err_msg += "Inbox, Sent Items, Drafts, Outbox, Junk, Deleted Items, Outbox"
                    Return Nothing
                    Exit Function
            End Select

            Console.WriteLine("root folder {0} found", root_folder.DisplayName)

            If folders.Count = 1 Then
                Return root_folder
            End If


            'loop through subfolders to drill to final folder
            Dim tgt_folder As Data.Folder

            Dim curr_root_folder As Data.Folder = root_folder

            For i = 1 To folders.Count - 1

                Dim sf As Data.SearchFilter = New Data.SearchFilter.ContainsSubstring(Data.FolderSchema.DisplayName, folders(i).ToString,
                                                                              Data.ContainmentMode.FullString, Data.ComparisonMode.IgnoreCase)
                Console.WriteLine("searching for folder {0}", folders(i).ToString)
                Dim aFolders As Data.FindFoldersResults = ews.FindFolders(curr_root_folder.Id, sf, New Data.FolderView(1))

                If aFolders.Count > 0 Then
                    tgt_folder = aFolders.Folders.Item(0)
                    curr_root_folder = tgt_folder
                Else
                    err_msg = "Target Folder [" + folder_path + "] not found."
                    Return Nothing
                    Exit Function
                End If

            Next


            'For i = 1 To folders.Count - 1
            '    Dim ewsFolderView As New Data.FolderView(9999)

            '    ewsFolderView.Traversal = Data.FolderTraversal.Deep
            '    Dim cf = msEws.Data.Folder.Bind(ews, curr_root_folder.Id)
            '    Dim fFoldersRes = cf.FindFolders(ewsFolderView)



            '    Console.WriteLine("sub folder {0} found", cf.DisplayName)


            '    'reset target folder
            '    tgt_folder = Nothing
            '    For Each f In fFoldersRes
            '        If f.DisplayName.ToLower = folders(i).ToString.ToLower Then
            '            tgt_folder = f
            '            curr_root_folder = tgt_folder
            '            Exit For
            '        End If
            '    Next

            '    If tgt_folder Is Nothing Then
            '        err_msg = "Target Folder [" + folder_path + "] not found."
            '        Return Nothing
            '        Exit Function
            '    End If

            'Next

            Return tgt_folder

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Function

    Private Shared Function _get_exchange_vsn(vsn As String) As msEws.Data.ExchangeVersion
        Select Case vsn.ToLower
            Case "2007_sp1"
                Return msEws.Data.ExchangeVersion.Exchange2007_SP1
            Case "2010"
                Return msEws.Data.ExchangeVersion.Exchange2010
            Case "2010_sp1"
                Return msEws.Data.ExchangeVersion.Exchange2010_SP1
            Case "2010_sp2"
                Return msEws.Data.ExchangeVersion.Exchange2010_SP2
            Case "2013"
                Return msEws.Data.ExchangeVersion.Exchange2013
            Case "2013_sp1"
                Return msEws.Data.ExchangeVersion.Exchange2013_SP1
                'Case Else 'return current if we dont know
                '    Return msEws.Data.ExchangeVersion.Exchange2013_SP1
        End Select



    End Function

End Class
