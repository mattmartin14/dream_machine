
Imports System.Net

Class cWebTasks

    Shared Sub webTask_main(ByRef err_msg As String)

        Dim task As String = LCase(EDIS.get_edis_pval("sub_task"))

        Select Case task
            Case "web_req"
                get_web_req_content(err_msg)
            Case "soap_req"
                Dim url As String = EDIS.get_edis_pval("url")
                Dim soap_envelope As String = EDIS.get_edis_pval("soap_envelope")
                Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")
                Dim encoding As String = EDIS.get_edis_pval("encoding")
                Dim content_type As String = EDIS.get_edis_pval("content_type")
                Dim svc_uid As String = EDIS.get_edis_pval("svc_uid")
                Dim svc_pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim timeout As Integer = CInt(EDIS.get_edis_pval("timeout"))
                'getSoapReq(soap_envelope, url, output_tbl_nm, mssql_inst, encoding, svc_uid, svc_pwd, content_type, timeout, err_msg)

        End Select
    End Sub

    Private Shared Sub get_web_req_content(ByRef err_msg As String)

        Try

            'params
            Dim url As String = EDIS.get_edis_pval("url")
            Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
            Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")
            Dim encoding As String = EDIS.get_edis_pval("encoding")
            Dim timeout As Integer = CInt(EDIS.get_edis_pval("timeout"))
            Dim svc_uid As String = EDIS.get_edis_pval("svc_uid")
            Dim svc_pwd As String = EDIS.get_edis_pval("svc_pwd")
            Dim rq_method As String = EDIS.get_edis_pval("method")
            Dim token_auth_url As String = EDIS.get_edis_pval("token_auth_url")

            Dim use_token_auth As Boolean = False
            If Trim(token_auth_url) <> "" Then
                use_token_auth = True
            End If

            Dim rq As WebRequest

            If use_token_auth = True Then
                rq = WebRequest.Create(token_auth_url)
            Else
                rq = WebRequest.Create(url)
            End If

            rq.Method = rq_method

            'add headers
            If EDIS.web_request_headers.Count > 0 Then
                For Each header In EDIS.web_request_headers
                    rq.Headers.Add(header.Key.ToString, header.Value.ToString)
                Next
            End If

            If svc_uid <> "" Then
                rq.Credentials = New NetworkCredential(svc_uid, svc_pwd)
            End If

            If timeout <> 0 Then
                rq.Timeout = timeout
            End If

            Dim webResp As WebResponse = rq.GetResponse()
            Dim content As String
            Dim content_stream As IO.StreamReader

            content_stream = New IO.StreamReader(webResp.GetResponseStream, get_encoding(encoding))
            content = content_stream.ReadToEnd()
            content_stream.Close()

            webResp.Close()

            Dim final_content As String = ""

            'if we are using token authentication, grab the token and go again
            If use_token_auth = True Then
                Dim access_token As String = Replace(content, "{""access_token"":""", "")
                access_token = Replace(access_token, """}", "")

                access_token = "bearer " + access_token

                Dim rq1 As WebRequest = WebRequest.Create(url)

                rq1.Headers.Add("Authorization", access_token)

                Dim webResp1 As WebResponse = rq1.GetResponse()
                Dim content_stream1 As IO.StreamReader
                content_stream1 = New IO.StreamReader(webResp1.GetResponseStream, get_encoding(encoding))
                final_content = content_stream1.ReadToEnd()
                content_stream1.Close()
                webResp1.Close()
            Else
                final_content = content
            End If


            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                Dim sql_str As String = "insert [" + output_tbl_nm + "] values (@val)"
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                    cmd.CommandTimeout = 0
                    cmd.Parameters.AddWithValue("@val", final_content)
                    cmd.ExecuteNonQuery()
                End Using
            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Private Shared Function get_encoding(ByVal encoding As String) As Text.Encoding
        Dim encoding_schema As Text.Encoding

        Select Case LCase(encoding)
            Case "utf7"
                encoding_schema = Text.Encoding.UTF7
            Case "utf8"
                encoding_schema = Text.Encoding.UTF8
            Case "utf32"
                encoding_schema = Text.Encoding.UTF32
            Case "unicode"
                encoding_schema = Text.Encoding.Unicode
            Case "bigendian"
                encoding_schema = Text.Encoding.BigEndianUnicode
            Case "ascii"
                encoding_schema = Text.Encoding.ASCII
            Case Else
                encoding_schema = Text.Encoding.Default
        End Select

        Return encoding_schema
    End Function

End Class
