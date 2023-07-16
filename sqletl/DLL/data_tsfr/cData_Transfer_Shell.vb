Friend Class cData_Transfer_Shell

    Friend Shared Sub run_data_transfer_db_to_ff_out(src_sys_nm As String, src_cn_str As String, src_cn_provider As String, src_cn_sub_provider As String, src_platform As String, src_qry As String, file_path As String,
                                              col_delim As String, row_term As String, text_qual As String, append_to_existing As Boolean, truncate_timestamps_to_microseconds As Boolean,
                                              is_unicode As Boolean, file_code_page As String, ByRef err_msg As String)

        '---------------------------------------------------------------------------
        'SET ALL DATA TRANFER VARS

        'since this is not the calling proc, we will skip logging the row
        EDIS.skip_data_transfer_log_row = True

        EDIS.update_edis_pval("src_sys_nm", src_sys_nm)
        'EDIS.update_edis_pval("src_sys_nm", "EDIS_HOST_MSSQL")
        EDIS.update_edis_pval("src_qry", src_qry)
        EDIS.update_edis_pval("src_server_provider", src_cn_provider)
        EDIS.update_edis_pval("src_server_sub_provider", src_cn_sub_provider)
        EDIS.update_edis_pval("src_platform", src_platform)
        'EDIS.update_edis_pval("src_platform", "MSSQL")
        EDIS.update_edis_pval("src_cn_str", src_cn_str)
        EDIS.update_edis_pval("dest_sys_nm", "FLATFILE")
        EDIS.update_edis_pval("dest_server_provider", "FLATFILE")
        EDIS.update_edis_pval("dest_server_sub_provider", "")
        'EDIS.update_edis_pval("dest_cn_type", "FLATFILE")
        EDIS.update_edis_pval("dest_platform", "FLATFILE")
        EDIS.update_edis_pval("dest_cn_str", file_path)
        EDIS.update_edis_pval("dest_tbl", "")
        EDIS.update_edis_pval("load_batch_size", 10000)
        EDIS.update_edis_pval("crt_dest_tbl", "0")
        EDIS.update_edis_pval("file_path", file_path)
        Dim append_to_existing_str As String
        If append_to_existing Then
            append_to_existing_str = "1"
        Else
            append_to_existing_str = "0"
        End If
        EDIS.update_edis_pval("append_to_existing_file", append_to_existing_str)
        EDIS.update_edis_pval("col_delim", col_delim)
        EDIS.update_edis_pval("row_term", row_term)
        EDIS.update_edis_pval("header_rows_to_skip", "0")
        EDIS.update_edis_pval("data_rows_to_skip", "0")
        EDIS.update_edis_pval("include_headers", "1")
        EDIS.update_edis_pval("include_ghost_col", "0")
        EDIS.update_edis_pval("max_col_width", 255)
        EDIS.update_edis_pval("load_on_ordinal", "0")
        EDIS.update_edis_pval("fixed_width_row_len", "0")
        EDIS.update_edis_pval("fixed_width_intervals", "")
        EDIS.update_edis_pval("ragged_right_intervals", "")
        If is_unicode Then
            EDIS.update_edis_pval("is_unicode", "1")
        Else
            EDIS.update_edis_pval("is_unicode", "0")
        End If

        EDIS.update_edis_pval("is_key_ordered", "0")

        EDIS.update_edis_pval("keep_ident", "0")
        EDIS.update_edis_pval("keep_nulls", "0")
        EDIS.update_edis_pval("pkg_file_path", "")
        EDIS.update_edis_pval("include_row_id_col", "0")
        EDIS.update_edis_pval("col_place_holder", "")
        EDIS.update_edis_pval("mssql_inst", EDIS._mssql_inst)
        EDIS.update_edis_pval("src_pre_sql_cmd", "")
        EDIS.update_edis_pval("is_key_ordered", "0")
        'EDIS.update_edis_pval("host_mssql_inst", EDIS._mssql_inst)

        If truncate_timestamps_to_microseconds Then
            EDIS.update_edis_pval("truncate_timestamps_to_microseconds", "1")
        Else
            EDIS.update_edis_pval("truncate_timestamps_to_microseconds", "0")
        End If

        EDIS.update_edis_pval("file_code_page", file_code_page)

        Main.log_edis_event("info", "params for dt ff out loaded", "done")

        data_tsfr.data_tsfr_task(err_msg)

    End Sub


End Class
