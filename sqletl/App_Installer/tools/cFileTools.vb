
Imports System.IO

Public Class cFileTools

    Public Shared Sub delete_file(ByVal file_path As String, ByRef err_msg As String)

        Try
            If File.Exists(file_path) Then
                File.Delete(file_path)
            End If
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub

End Class
