Imports Microsoft.SqlServer.Server
Public Class cEntry
    <SqlProcedure>
    Public Shared Sub main(exec_id As String)
        Try
            'Dim err_msg As String
            cMain.main(exec_id)


            'cExeLauncher.run_edis(exec_id, sString1, sString2)


        Catch ex As Exception
            Throw New Exception(ex.Message.ToString())
        End Try

    End Sub
End Class
