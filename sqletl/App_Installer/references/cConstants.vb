

Public Class cConstants

    Public Shared sql_version_year As String = ""
    Public Shared dts_folder_nbr As String = ""
    Public Shared edis_activation_type As String = ""
    Public Shared edis_serial_nbr As String = ""
    Public Shared edis_user_version As String = ""
    Public Shared full_sql_instance_name As String = ""

    Public Const prod_version = "EDIS 7.4.20180327 (fixed BigQuery Bugs)"
    Public Const release_notes As String = "
Bug Fixes: 
    1) Fixes SharePoint row limit constraint when reading lists
    2) Fixes SharePoint timestamp for created/modified to reflect the local time zone
Enhancements
    1) Adds Support for connecting to MSOLAP (SSAS) Cubes
"

    Public Const use_encryption As Boolean = True

    Public Shared copyright_notice As String = <![CDATA[
        
    /*
		**************************************************************************************************************************************
		******												COPYRIGHT NOTICE        													******
		**************************************************************************************************************************************
                Copyright © 2017 SQLETL.COM

                All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means
                without the prior written permission of MD Data Technologies, LLC.

                <<< NOTE >>>
                If you modify the code below, you void any and all support from MD Data Tech. We cannot offer support if you change the code.

                
        **************************************************************************************************************************************
		******												COPYRIGHT NOTICE        													******
		**************************************************************************************************************************************
	*/

    ]]>.Value.ToString()


    Shared Sub initialize_global_vars(ByVal mssql_inst As String)

        sql_version_year = mSQLFunctions.get_sql_vsn_year(mssql_inst)
        dts_folder_nbr = mSQLFunctions.get_server_version(mssql_inst)
        full_sql_instance_name = mSQLFunctions.get_mssql_inst_full_nm(mssql_inst)


    End Sub

End Class
