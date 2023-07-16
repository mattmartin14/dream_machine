Imports System.Net
Class EDISWBTasks


    Shared Sub webTask_main(ByRef err_msg As String)

        Dim task As String = LCase(EDIS.get_edis_pval("sub_task"))

        Dim svc_id As String = EDIS.get_edis_pval("svc_id")
        Dim svc_uid As String = EDIS.get_edis_pval("svc_uid")
        Dim svc_pwd As String = EDIS.get_edis_pval("svc_pwd")

        Select Case task
            Case "web_req"
                Dim url As String = EDIS.get_edis_pval("url")
                Dim soap_envelope As String = EDIS.get_edis_pval("soap_envelope")
                Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")
                Dim encoding As String = EDIS.get_edis_pval("encoding")
                Dim content_type As String = EDIS.get_edis_pval("content_type")
                Dim proxy_url As String = EDIS.get_edis_pval("proxy_url")
                Dim proxy_svc_uid As String = EDIS.get_edis_pval("proxy_svc_uid")
                Dim proxy_svc_pwd As String = EDIS.get_edis_pval("proxy_svc_pwd")
                Dim accept As String = EDIS.get_edis_pval("accept")
                Dim proxy_port As Integer = EDIS.get_edis_pval("proxy_port")


                Dim timeout As Integer = CInt(EDIS.get_edis_pval("timeout"))
                Dim rq_method As String = EDIS.get_edis_pval("method")
                Dim token_auth_url As String = EDIS.get_edis_pval("token_auth_url")
                Dim tls_version As String = EDIS.get_edis_pval("tls_version")
                get_web_req_content(url, output_tbl_nm, mssql_inst, encoding, timeout, svc_uid, svc_pwd, rq_method, token_auth_url, tls_version, EDIS.web_request_headers, err_msg,
                                    proxy_url, proxy_svc_uid, proxy_svc_pwd, accept, content_type, proxy_port)
            Case "soap_req"
                Dim url As String = EDIS.get_edis_pval("url")
                Dim soap_envelope As String = EDIS.get_edis_pval("soap_envelope")
                Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")
                Dim encoding As String = EDIS.get_edis_pval("encoding")
                Dim content_type As String = EDIS.get_edis_pval("content_type")

                Dim timeout As Integer = CInt(EDIS.get_edis_pval("timeout"))

                getSoapReq(soap_envelope, url, output_tbl_nm, mssql_inst, encoding, svc_uid, svc_pwd, content_type, timeout, err_msg)

        End Select

    End Sub

    Private Shared Sub get_web_req_content(url As String, output_tbl_nm As String, mssql_inst As String, encoding As String, timeout As String, svc_uid As String, svc_pwd As String, rq_method As String,
                                token_auth_url As String, tls_version As String, headers As Dictionary(Of String, String), ByRef err_msg As String,
                                           proxy_url As String, proxy_svc_uid As String, proxy_svc_pwd As String, accept As String, content_type As String, proxy_port As Integer)

        Try

            'params

            Dim use_token_auth As Boolean = False
            If Trim(token_auth_url) <> "" Then
                use_token_auth = True
            End If

            Dim rq As HttpWebRequest

            'force up the tls version if required
            If tls_version = "1.2" Then
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
            ElseIf tls_version = "1.1" Then
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11
            End If

            If use_token_auth = True Then
                rq = HttpWebRequest.Create(token_auth_url)
            Else
                rq = HttpWebRequest.Create(url)
            End If

            rq.Method = rq_method

            If Not String.IsNullOrWhiteSpace(content_type) Then
                rq.ContentType = content_type
            End If

            If Not String.IsNullOrWhiteSpace(accept) Then
                rq.Accept = accept
            End If

            'add headers
            If headers.Count > 0 Then
                For Each header In headers
                    rq.Headers.Add(header.Key.ToString, header.Value.ToString)
                Next
            End If

            If Not String.IsNullOrWhiteSpace(svc_uid) Then
                rq.Credentials = New NetworkCredential(svc_uid, svc_pwd)
            End If

            If timeout <> 0 Then
                rq.Timeout = timeout
            End If

            Dim proxy As WebProxy

            If Not String.IsNullOrWhiteSpace(proxy_url) Then
                If proxy_port <> -1 Then
                    proxy = New WebProxy(proxy_url, proxy_port)
                Else
                    proxy = New WebProxy(proxy_url)
                End If
            End If

            If Not String.IsNullOrWhiteSpace(proxy_svc_uid) Then
                Dim proxy_creds As NetworkCredential = New NetworkCredential(proxy_svc_uid, proxy_svc_pwd)
                proxy.Credentials = proxy_creds
                proxy.BypassProxyOnLocal = False
            End If

            If Not String.IsNullOrWhiteSpace(proxy_url) Then
                rq.Proxy = proxy
            End If

            Dim webResp As HttpWebResponse = rq.GetResponse()
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

                Dim rq1 As HttpWebRequest = HttpWebRequest.Create(url)

                rq1.Headers.Add("Authorization", access_token)

                'add proxy if using
                If Not String.IsNullOrWhiteSpace(proxy_url) Then
                    rq1.Proxy = proxy
                End If

                Dim webResp1 As HttpWebResponse = rq1.GetResponse()
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
            err_msg = ex.GetBaseException.ToString
            'err_msg = ex.Message.ToString
        End Try

    End Sub

    Private Shared Sub getSoapReq(ByVal soap_envelope As String, ByVal url As String, ByVal output_tbl_nm As String, ByVal mssql_inst As String, ByVal encoding As String _
           , ByVal svc_uid As String, ByVal svc_pwd As String, content_type As String, ByVal timeout As Integer, ByRef err_msg As String
       )

        Try
            Dim webReq As HttpWebRequest = WebRequest.Create(url)

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

            Dim buffer() As Byte = encoding_schema.GetBytes(soap_envelope)
            Dim content As String

            webReq.AllowWriteStreamBuffering = False
            webReq.Method = "POST"
            webReq.ContentType = content_type ' "text/xml;charset=UTF-8"
            webReq.Headers.Add("SOAPAction", "POST")
            webReq.ContentLength = buffer.Length

            If timeout <> 0 Then
                webReq.Timeout = timeout
            End If

            If svc_uid <> "" Then
                webReq.Credentials = New NetworkCredential(svc_uid, svc_pwd)
            End If

            Dim post As IO.Stream = webReq.GetRequestStream

            post.Write(buffer, 0, buffer.Length)
            post.Close()

            Dim webResp As HttpWebResponse = webReq.GetResponse
            Dim reader As New IO.StreamReader(webResp.GetResponseStream, encoding_schema)
            content = reader.ReadToEnd
            reader.Close()

            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                Dim sql_str As String = "insert [" + output_tbl_nm + "] values (@val)"
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                    cmd.CommandTimeout = 0
                    cmd.Parameters.AddWithValue("@val", content)
                    cmd.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub
    Private Shared Sub get_web_req_content_old(url As String, output_tbl_nm As String, mssql_inst As String, encoding As String, timeout As String, svc_uid As String, svc_pwd As String, rq_method As String,
                                token_auth_url As String, tls_version As String, headers As Dictionary(Of String, String), ByRef err_msg As String)

        Try

            'params

            Dim use_token_auth As Boolean = False
            If Trim(token_auth_url) <> "" Then
                use_token_auth = True
            End If

            Dim rq As WebRequest

            'force up the tls version if required
            If tls_version = "1.2" Then
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
            ElseIf tls_version = "1.1" Then
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11
            End If

            If use_token_auth = True Then
                rq = WebRequest.Create(token_auth_url)
            Else
                rq = WebRequest.Create(url)
            End If

            rq.Method = rq_method

            'add headers
            If headers.Count > 0 Then
                For Each header In headers
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
