Class cADO_Type

    Shared Function get_assembly_name(input_type As String) As String

        Dim res As String = ""
        Select Case input_type
            Case "ODBC"
                res = String.Format("ADO.NET:{0}", GetType(System.Data.Odbc.OdbcConnection).AssemblyQualifiedName)
            Case "OLEDB", "EXCEL"
                res = String.Format("ADO.NET:{0}", GetType(System.Data.OleDb.OleDbConnection).AssemblyQualifiedName)
        End Select

        Return res

    End Function

End Class
