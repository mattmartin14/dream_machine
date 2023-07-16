
Imports System.Security.Cryptography
Imports System.Text
Imports Microsoft.Win32

'--------- CREATING A NEW COPY OF THE PROJECT
'When Loading this project to a new copy, do the following:
'   1) Use nuget to get LicenseSpot

Public Module mMain

    Public Sub Main(ByVal args As String())

        Dim err_msg As String = ""

        Try


            '########## TESTING ######################################
            'ReDim args(3)
            'args(0) = "MDDT\DEV16"
            'args(1) = "trial"
            'args(2) = "C:\temp\blah_test.txt"
            'args(3) = "install"
            '########## TESTING ######################################

            Dim mssql_inst As String = args(0).ToString
            Dim serial_nbr As String = args(1).ToString
            Dim write_log_path As String = args(2).ToString
            Dim request As String = args(3).ToString '"install'

            Call install(mssql_inst, serial_nbr, err_msg)


            'write out the error message (if applicable)
            'offline?
            If LCase(Left(err_msg, 8)) = "offline:" Then
                err_msg = err_msg
            ElseIf err_msg <> "" Then
                err_msg = "error: " + err_msg
            End If
            Dim file As System.IO.StreamWriter
            file = My.Computer.FileSystem.OpenTextFileWriter(write_log_path, False)
            file.WriteLine(err_msg)
            file.Close()
        Catch ex As Exception
            Console.WriteLine("Error running installation: " + ex.Message.ToString())
        End Try

    End Sub



    Private Sub install(ByVal mssql_inst As String, serial_nbr As String, ByRef err_msg As String)

        Try

            'license validation
            If serial_nbr <> "trial" Then
                cCheckLicense.check_license(serial_nbr, mssql_inst, err_msg)
                If err_msg <> "" Then GoTo report_error
            End If

            'Create Hash Key
            Dim hash As String = generate_license_key(mssql_inst, serial_nbr, err_msg)
            If err_msg <> "" Then GoTo report_error

            '-----------------------------------------------------------------------------------------------------------------------------------------------
            'Install MDDatatech Folder and Environment

            'Rev 2016/10/13: Redirect to Registry..not MSSQL

            create_mddt_reg_key(mssql_inst, serial_nbr, hash, err_msg)
            If err_msg <> "" Then GoTo report_error
            Exit Sub

report_error:
            'we exit here and pass the error message back to the parent
            Exit Sub

        Catch ex As Exception
            err_msg = ex.Message
        End Try

    End Sub

    Private Sub create_mddt_reg_key(ByVal mssql_inst As String, ByVal serial_nbr As String, ByVal install_id As String, ByRef err_msg As String)

        Try
            Dim baseKey As RegistryKey
            If Environment.Is64BitOperatingSystem Then
                baseKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64)
            Else
                baseKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry32)
            End If

            Dim key = baseKey.CreateSubKey("Software\SQLETL.COM\" + Replace(LCase(mssql_inst), "\", "_"))

            key.SetValue("EDIS Install ID", LCase(install_id))
            key.SetValue("Install TS", Now.ToShortDateString + " " + Now.ToShortTimeString)

            key.Close()
            baseKey.Close()
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Private Function generate_license_key(ByVal mssql_inst As String, ByVal serial_nbr As String, ByRef err_msg As String) As String

        Dim ret_val As String = ""

        Try
            Dim sql_srvr_name As String = Replace(mssql_inst, "\", "_")

            Using md5hash As MD5 = MD5.Create()

                Dim hash_string As String
                If serial_nbr = "trial" Then
                    hash_string = sql_srvr_name + "_%@KSNBS)NCGE_TRIAL"
                Else
                    hash_string = sql_srvr_name + "_XL$#NCAG$%*&_FULL"
                End If

                Dim hashed_value As String = GetMd5Hash(md5hash, hash_string)
                ret_val = hashed_value
            End Using

        Catch ex As Exception
            err_msg = ex.Message
        End Try

        Return ret_val

    End Function

    Private Function GetMd5Hash(ByVal md5Hash As MD5, ByVal input As String) As String

        ' Convert the input string to a byte array and compute the hash.
        Dim data As Byte() = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(input))

        ' Create a new Stringbuilder to collect the bytes
        ' and create a string.
        Dim sBuilder As New StringBuilder()

        ' Loop through each byte of the hashed data 
        ' and format each one as a hexadecimal string.
        Dim i As Integer
        For i = 0 To data.Length - 1
            sBuilder.Append(data(i).ToString("x2"))
        Next i

        ' Return the hexadecimal string.
        Return sBuilder.ToString()

    End Function 'GetMd5Hash

End Module
