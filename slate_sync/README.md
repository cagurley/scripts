# README

For use only with the intended Slate and PeopleSoft databases. If using the compiled executable, you must have a 64-bit OS with both a 64-bit SQL Server ODBC driver and 64-bit Oracle client software installed on your local machine.

Connecting requires the availability of two well-formed JSON files in a directory named `slate_sync_vars` in the top level of your HOME directory (this is generally the directory corresponding to your user on your local machine) or the "C:" drive (if using this option __this folder cannot exist in the former location__). The files and their formats, wherein each capscase word in curly braces should be substituted with your unique values, are:

`connect.json`

```
{
  "sqlserver": {
    "driver": "{DRIVERNAME}",
    "host": "{HOST},{PORT}",
    "database": "{DATABASE}",
    "user": "{USER}",
    "password": "{PASSWORD}"
  },
  "oracle": {
    "user": "{USER}",
    "password": "{PASSWORD}",
    "host": "{HOST}",
    "port": {PORT},
    "service_name": "{SERVICENAME}"
  }
}
```

The SQL Server `{DRIVERNAME}` string should be of the form `{ODBC Driver N for SQL Server}`, where `N` should be replaced with the installed driver version (note that the braces here are literal and should be included in the file); to check the versions installed on Windows, open "ODBC Data Sources (64-bit)" and check the "Drivers" tab. Connecting to Oracle is accomplished with a `tnsnames.ora` configuration, which will also provide you the relevant connection information (note that the `{PORT}` value should be the only unquoted value in the file); this file is usually found in the `/network/admin` directory buried in your Oracle client subdirectory (the root of these Oracle directories is typically located in the root of your home drive).

`qvars.json`

```
{
  "oracle": {
    "termlb": "{TERMLOWERBOUND}",
    "termub": "{TERMUPPERBOUND}"
  }
}
```
