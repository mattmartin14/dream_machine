
Module mCn_builder

    Function build_cn_str(ByVal sys_type As String, ByVal file_path As String, ByVal is_src As Boolean,
                          Optional ByVal include_headers As Boolean = False) As String


        '------------------------------------------------------------------------------------------------------------------

        Dim provider As String
        If Right(file_path, 4).ToLower = ".xls" Or Right(file_path, 4).ToLower = ".mdb" Then
            provider = "Provider = Microsoft.JET.OLEDB.4.0; "
        Else
            provider = "Provider = Microsoft.ACE.OLEDB." + EDIS.get_edis_pval("ace_engine_version") + ".0; "
        End If

        Dim ext_props As String = ""
        Dim data_src As String = "Data Source = " & file_path & ";"
        Dim hdr_ind As String = ""
        Dim imex_handler As String = ""
        Dim cn_str As String = ""

        Select Case LCase(sys_type)
            Case "excel"
                Select Case Right(LCase(file_path), 4)
                    Case "xlsx"
                        ext_props = " Extended Properties = ""Excel 12.0 XML;"
                    Case "xlsm"
                        ext_props = " Extended Properties = ""Excel 12.0 Macro;"
                    Case "xlsb"
                        ext_props = " Extended Properties = ""Excel 12.0;"
                    Case ".xls"
                        ext_props = " Extended Properties = ""Excel 8.0;"
                End Select

                If include_headers Then
                    hdr_ind = "HDR=YES;"
                Else
                    hdr_ind = "HDR=NO;"
                End If

                If is_src Then
                    imex_handler = "IMEX=1"
                Else
                    imex_handler = "IMEX=0" 'zero for dest
                End If

                cn_str = provider & data_src & ext_props & hdr_ind & imex_handler & """" & ";"

            Case "access"
                cn_str = provider & data_src & " Persist Security Info=False;"
                'cn_str = "Provider = Microsoft.ACE.OLEDB.12.0; Data Source = " & file_path & "; Persist Security Info=False; "
        End Select

        Return cn_str

    End Function

End Module