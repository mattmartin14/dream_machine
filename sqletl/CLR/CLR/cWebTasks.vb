
Imports System.Net
Imports System.Threading

Public Class cWebTasks
    Shared Sub main(params As Dictionary(Of String, String), web_req_headers As Dictionary(Of String, String), ByRef err_msg As String)

        Dim sub_task As String = cMain.get_edis_pval(params, "sub_task").ToLower

        Select Case sub_task
            Case "web_req"
                get_web_req_content(params, web_req_headers, err_msg)
            Case Else
                err_msg = "web request task not found"
        End Select

    End Sub

    Private Shared Sub get_web_req_content(params As Dictionary(Of String, String), web_req_headers As Dictionary(Of String, String), ByRef err_msg As String)

        Dim user_cancel As Boolean = False

        Dim rq As HttpWebRequest
        Dim rq1 As HttpWebRequest

        Try

            'params
            Dim svc_id As String = cMain.get_edis_pval(params, "svc_id")
            Dim svc_uid As String = cMain.get_edis_pval(params, "svc_uid")
            Dim svc_pwd As String = cMain.get_edis_pval(params, "svc_pwd")
            Dim url As String = cMain.get_edis_pval(params, "url")
            Dim output_tbl_nm As String = cMain.get_edis_pval(params, "output_tbl_nm")
            Dim mssql_inst As String = cMain.get_edis_pval(params, "mssql_inst")
            Dim encoding As String = cMain.get_edis_pval(params, "encoding")
            Dim content_type As String = cMain.get_edis_pval(params, "content_type")
            Dim proxy_url As String = cMain.get_edis_pval(params, "proxy_url")
            Dim proxy_svc_uid As String = cMain.get_edis_pval(params, "proxy_svc_uid")
            Dim proxy_svc_pwd As String = cMain.get_edis_pval(params, "proxy_svc_pwd")
            Dim accept As String = cMain.get_edis_pval(params, "accept")
            Dim proxy_port As Integer = cMain.get_edis_pval(params, "proxy_port")



            Dim timeout As Integer = CInt(cMain.get_edis_pval(params, "timeout"))
            Dim rq_method As String = cMain.get_edis_pval(params, "method")
            Dim token_auth_url As String = cMain.get_edis_pval(params, "token_auth_url")
            ' Dim tls_version As String = cMain.get_edis_pval(params, "tls_version")


            Dim use_token_auth As Boolean = False
            If Trim(token_auth_url) <> "" Then
                use_token_auth = True
            End If

            'Dim rq As HttpWebRequest

            'in CLR, this is not supported since it requires .NET 4.5 or higher and CLR only runs on .NET 4.0
            ''force up the tls version if required
            'If tls_version = "1.2" Then
            '    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
            'ElseIf tls_version = "1.1" Then
            '    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11
            'End If

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
            If web_req_headers.Count > 0 Then
                For Each header In web_req_headers
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

            Dim content As String

            Using webResp As HttpWebResponse = rq.GetResponse()
                Using content_stream As IO.StreamReader = New IO.StreamReader(webResp.GetResponseStream, get_encoding(encoding))
                    content = content_stream.ReadToEnd()
                    content_stream.Close()
                    webResp.Close()
                End Using
            End Using

            'Dim content As String
            'Dim content_stream As IO.StreamReader

            'content_stream = New IO.StreamReader(webResp.GetResponseStream, get_encoding(encoding))
            'content = content_stream.ReadToEnd()
            'content_stream.Close()

            'webResp.Close()

            Dim final_content As String = ""

            'if we are using token authentication, grab the token and go again
            If use_token_auth = True Then
                Dim access_token As String = Replace(content, "{""access_token"":""", "")
                access_token = Replace(access_token, """}", "")

                access_token = "bearer " + access_token

                rq1 = HttpWebRequest.Create(url)

                rq1.Headers.Add("Authorization", access_token)

                'add proxy if using
                If Not String.IsNullOrWhiteSpace(proxy_url) Then
                    rq1.Proxy = proxy
                End If

                Using webResp1 As HttpWebResponse = rq1.GetResponse()
                    Using content_stream1 As IO.StreamReader = New IO.StreamReader(webResp1.GetResponseStream, get_encoding(encoding))
                        final_content = content_stream1.ReadToEnd()
                        content_stream1.Close()
                        webResp1.Close()
                    End Using
                End Using

                'Dim webResp1 As HttpWebResponse = rq1.GetResponse()
                'Dim content_stream1 As IO.StreamReader
                'content_stream1 = New IO.StreamReader(webResp1.GetResponseStream, get_encoding(encoding))
                'final_content = content_stream1.ReadToEnd()
                'content_stream1.Close()
                'webResp1.Close()
            Else
                final_content = content
            End If

            rq = Nothing
            rq1 = Nothing


            Using cn As New SqlClient.SqlConnection("context connection = true")
                Dim sql_str As String = "insert [" + output_tbl_nm + "] values (@val)"
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                    cmd.CommandTimeout = 0
                    cmd.Parameters.AddWithValue("@val", final_content)
                    cmd.ExecuteNonQuery()
                End Using
            End Using

        Catch ab As ThreadAbortException
            user_cancel = True

        Catch ex As Exception
            err_msg = ex.GetBaseException.ToString()
        Finally
            If user_cancel Then
                rq = Nothing
                rq1 = Nothing
            End If
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
