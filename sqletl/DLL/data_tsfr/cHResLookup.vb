Imports System.Reflection
Imports Microsoft.SqlServer.Dts.Runtime
Friend Class cHResLookup

    Friend Shared Function get_com_symbolic_err_msg(errorCode As Integer) As String
        Dim symbolicName As String = String.Empty
        Dim hresults As New HResults()

        For Each fieldInfo As FieldInfo In hresults.[GetType]().GetFields()
            If CInt(fieldInfo.GetValue(hresults)) = errorCode Then
                symbolicName = fieldInfo.Name
                Exit For
            End If
        Next

        Return symbolicName
    End Function

End Class
