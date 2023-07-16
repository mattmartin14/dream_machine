
Imports System.Net
Public Class cWebTasks

    Public Shared Sub getSoapReqPub(ByVal soap_envelope As String, ByVal url As String, ByVal output_tbl_nm As String, ByVal mssql_inst As String, ByVal encoding As String _
            , ByVal svc_uid As String, ByVal svc_pwd As String, content_type As String, ByVal timeout As Integer, ByRef err_msg As String)

        getSoapReq(soap_envelope, url, output_tbl_nm, mssql_inst, encoding, svc_uid, svc_pwd, content_type, timeout, err_msg)

    End Sub

    Public Shared Sub get_web_req_contentPub(url As String, output_tbl_nm As String, mssql_inst As String, encoding As String, timeout As String, svc_uid As String, svc_pwd As String, rq_method As String,
                                token_auth_url As String, tls_version As String, headers As Dictionary(Of String, String), ByRef err_msg As String)


        get_web_req_content(url, output_tbl_nm, mssql_inst, encoding, timeout, svc_uid, svc_pwd, rq_method, token_auth_url, tls_version, headers, err_msg)

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
    Private Shared Sub get_web_req_content(url As String, output_tbl_nm As String, mssql_inst As String, encoding As String, timeout As String, svc_uid As String, svc_pwd As String, rq_method As String,
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
