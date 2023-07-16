
Imports System.Security.Cryptography
Imports System.Text

Public Class Form1

    Private Sub btn_gen_offline_code_Click(sender As Object, e As EventArgs) Handles btn_gen_offline_code.Click

        Dim hardware_id As String, offline_code As String, err_msg As String = ""

        Try
            hardware_id = Trim(Me.txt_hardware_id.Text)
            offline_code = get_offline_cd(hardware_id, err_msg)
            Me.txt_offline_code.Text = offline_code
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

        If err_msg <> "" Then
            MsgBox(err_msg, vbCritical, "Error Generating Key")

        End If

    End Sub

    Private Shared Function get_offline_cd(ByVal hardware_id As String, ByRef err_msg As String) As String

        Dim res As String = ""
        Dim input_string As String = hardware_id + "_@LKNS*(BNQC)AKTKNGLNS(*$NB~@!K|{$(@NSBS.cgtfff---2fdin"

        Using sha1hash As SHA512 = SHA512Managed.Create()

            Dim data As Byte() = sha1hash.ComputeHash(Encoding.UTF8.GetBytes(input_string))
            Dim sBuilder As New StringBuilder()

            Dim i As Integer
            For i = 0 To data.Length - 1
                sBuilder.Append(data(i).ToString("x2"))
            Next i

            res = sBuilder.ToString()

        End Using

        Return res

    End Function

End Class

