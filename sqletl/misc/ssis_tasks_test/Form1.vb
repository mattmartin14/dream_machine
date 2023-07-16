Public Class Form1

    Friend Shared edis_params As New Dictionary(Of String, String)

    Private Sub Button1_Click(sender As Object, e As EventArgs) Handles Button1.Click

        edis_params.Add("pre_sql_cmd", "SELECT 1 as GOT_IT")

        cSSIS_Tasks.main()

    End Sub

    Friend Shared Function get_edis_pval(ByVal param_nm) As String

        If edis_params.ContainsKey(param_nm) Then
            Return edis_params.Item(param_nm).ToString
        Else
            Return ""
        End If

    End Function
End Class
