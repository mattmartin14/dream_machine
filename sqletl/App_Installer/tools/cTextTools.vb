
Imports System.Reflection
Imports System.IO
Imports System.String

Public Class cTextTools

    Public Shared Function get_temp_path() As String

        Dim temp_path As String = System.IO.Path.GetTempPath.ToString()
        Return temp_path

    End Function

    Public Shared Function get_temp_file_path() As String

        Dim tempfile As FileInfo = New FileInfo(Path.GetTempFileName())

        Return tempfile.FullName.ToString

    End Function

    Public Shared Function read_resource_txt_file(assembly_nm As String, ByRef err_msg As String) As String

        Dim res As String = ""

        Try
            'Get Install Script and run it
            Dim asm As Assembly = Assembly.GetExecutingAssembly

            'Get Install Script and run it
            Using stream As Stream = asm.GetManifestResourceStream(assembly_nm)
                Using sr As StreamReader = New StreamReader(stream)
                    res = sr.ReadToEnd()
                End Using
            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

        Return res

    End Function

    Public Shared Function read_file(file_path As String, ByRef err_msg As String) As String

        Dim res As String = ""
        Try

            Using sr As New StreamReader(file_path)
                res = sr.ReadToEnd()
            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

        Return res
    End Function

End Class
