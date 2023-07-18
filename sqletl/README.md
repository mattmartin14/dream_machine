# sqletl/EDIS
## This was my first professional application I built back in 2016

The application installs on SQL Servers versions 2012-2017. It programatically creates and executes SSIS packages on the fly from SQL commands. This helped accelerate ETL work for teams that did not want to learn SSIS and needed a way to take some simple sql templates, tweak some parameters and then move the data quick. Below is a simple example of how the user intefaces with this application:

```sql
  EXEC SSISDB.edis.usp_run_data_transfer @src_sys = 'MY_SOURCE_CONN_ALIAS', @des_sys = 'FLATFILE'
    ,@src_qry = 'SELECT * FROM TBL'
    ,@dest_file_nm = 'data_file.txt'
```

In the above example, the user is using SQL to tell the EDIS program to build an SSIS package on the fly programatically, set the source connector to an alias that they had already setup with the EDIS program called "MY_SOURCE_CONN_ALIAS", to issue a query against the source system, and to send those results to a text file called "data_file.txt".

Under the covers, the SSISDB launches a shell SSIS package. The shell package invokes the EDIS executable file and passes in a few paramaters so that the program knows how to build and configure the SSIS package on the fly. The main file that builds the SSIS package on the fly is located [here]().
