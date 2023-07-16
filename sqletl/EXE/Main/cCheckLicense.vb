
Imports SkyXoft.BusinessSolutions.LicenseManager.Protector
Imports System.Security.Cryptography
Imports System.Text

Class cCheckLicense

    Shared Sub check_license(ByVal serial_nbr As String, ByVal mssql_inst As String, ByRef err_msg As String)

        Dim msg As String = ""

        Dim license As ExtendedLicense

        license = ExtendedLicenseManager.GetLicense(Nothing, Nothing, lic_key_server)

        Dim license_activated As Boolean = False

        Dim hardware_id As String = generate_hardware_id(license.HardwareID.ToString, mssql_inst, err_msg)

        'is this an offline activation?
        If InStr(Trim(serial_nbr), "-") = 0 Then

            'Dim hash As String = generate_offline_key(license.HardwareID.ToString(), err_msg)
            Dim hash As String = generate_offline_key(hardware_id, err_msg)
            If err_msg <> "" Then Exit Sub

            If hash = Trim(serial_nbr) Then
                Exit Sub
            End If

        End If

        'activate
        Try
            license.Activate(serial_nbr, True)
            license_activated = True
            Exit Sub
        Catch ex As Exception

            'license already activated?
            If LCase(ex.Message) Like "*you have no activations left in your license*" Then

                err_msg = "Please note: You have already activated this program using license " & serial_nbr & "." & vbCrLf
                err_msg = err_msg + "To purchase additional licenses, please visit " + mConstants.mddt_website_adrs

                Exit Sub

                'bad license?
            ElseIf LCase(ex.Message) Like "*the license is invalid*" Then
                'Is the license already activated?
                err_msg = "Please note: The license you have provided [" & serial_nbr & "] is invalid." & vbCrLf
                err_msg = err_msg + "To purchase a license, please visit " + mConstants.mddt_website_adrs

                Exit Sub


                'offline?
            ElseIf LCase(ex.Message) Like "*the licensespot server cannot be reach at this time*" Then

offline_act_test:

                'err_msg = "offline:" + license.HardwareID.ToString()
                err_msg = "offline:" + hardware_id
                Exit Sub

                'unknown
            Else
                err_msg = "Note: There was an error while attempting to activate your product with the error message below. "
                err_msg += vbCrLf
                err_msg += "Please email " + mConstants.support_eml_adrs + " the info below. We will respond within 48 hours and apoligize for any inconvinience."
                err_msg += vbCrLf
                err_msg += "error: " + ex.Message()

                Exit Sub
            End If


        End Try

skip_license_check:


    End Sub

    Private Shared Function generate_hardware_id(ByVal hardware_id As String, ByVal mssql_inst As String, ByRef err_msg As String) As String

        Dim res As String = ""
        Dim input_string As String = hardware_id + mssql_inst

        Using sha1hash As SHA1 = SHA1Managed.Create()

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


    Private Shared Function generate_offline_key(ByVal hardware_id As String, ByRef err_msg As String) As String

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
