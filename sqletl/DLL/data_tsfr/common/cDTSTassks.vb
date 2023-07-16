Friend Class cDTSTassks

    Friend Shared Function add_cn(ByRef pkg As Microsoft.SqlServer.Dts.Runtime.Package, cn_type As String,
                                  cn_str As String, cn_name As String) As Microsoft.SqlServer.Dts.Runtime.ConnectionManager

        Dim cn As Microsoft.SqlServer.Dts.Runtime.ConnectionManager
        cn = pkg.Connections.Add(cn_type)
        cn.ConnectionString = cn_str
        cn.Name = cn_name



        Return cn

    End Function

End Class
