Public Class EDIS
    Public Shared edis_params As New Dictionary(Of String, String)
    Friend Shared web_request_headers As New Dictionary(Of String, String)
    Friend Shared Function get_edis_pval(ByVal param_nm) As String


        If EDIS.edis_params.ContainsKey(param_nm) Then
            Return EDIS.edis_params.Item(param_nm).ToString
        Else
            Return ""
        End If

    End Function
End Class

Public Class Form1




    Private Sub Button1_Click(sender As Object, e As EventArgs) Handles Button1.Click

        Dim err_msg As String = ""

        'reg TESTING WEB REQUEST
        EDIS.edis_params.Add("url", "http://www.google.com/finance/option_chain?q=AAPL&output=json")
        EDIS.edis_params.Add("output_tbl_nm", "##tmp123")
        EDIS.edis_params.Add("mssql_inst", "MDDT\DEV16")
        EDIS.edis_params.Add("encoding", "utf16")
        EDIS.edis_params.Add("timeout", "0")
        EDIS.edis_params.Add("svc_uid", "")
        EDIS.edis_params.Add("svc_pwd", "")
        EDIS.edis_params.Add("method", "GET")
        EDIS.edis_params.Add("token_auth_url", "")
        EDIS.edis_params.Add("sub_task", "web_req")

        ''TOKEN AUTHENTICATION TESTING WEB REQUEST
        'EDIS.edis_params.Add("url", "https://qa.stellarfinancial.com/ActivityReport/ActivityReport.svc/account/365927/20160101/20160219")
        'EDIS.edis_params.Add("output_tbl_nm", "##tmp123")
        'EDIS.edis_params.Add("mssql_inst", "MDDT\DEV16")
        'EDIS.edis_params.Add("encoding", "utf16")
        'EDIS.edis_params.Add("timeout", "0")
        'EDIS.edis_params.Add("svc_uid", "")
        'EDIS.edis_params.Add("svc_pwd", "")
        'EDIS.edis_params.Add("method", "GET")
        'EDIS.edis_params.Add("token_auth_url", "https://qa.stellarfinancial.com/STSA/STSAuth.svc/authorize/activityreport")
        'EDIS.edis_params.Add("sub_task", "web_req")
        'EDIS.web_request_headers.Add("Authorization", "basic ncfnew:asdf123")


        'EDIS.edis_params.Add("sub_task", "read_sharepoint_list")
        'EDIS.edis_params.Add("batch_size", "12")
        'EDIS.edis_params.Add("sharepoint_list_nm", "avt_persons")
        'EDIS.edis_params.Add("sharepoint_url", "https://netorgft762028.sharepoint.com/EDIS_DEV/")
        'EDIS.edis_params.Add("svc_uid", "matt@sqletl.com")
        'EDIS.edis_params.Add("svc_pwd", "")
        'EDIS.edis_params.Add("sharepoint_version", "online")
        'EDIS.edis_params.Add("EDIS_MSSQL_INST", "MDDT\DEV16")
        'EDIS.edis_params.Add("output_tbl_nm", "##tmp123")

        'cSharePoint.sharepoint_main(err_msg)

        cWebTasks.webTask_main(err_msg)

    End Sub


End Class
