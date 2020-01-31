# README

For use only with the intended Slate and PeopleSoft databases.

Connecting requires the availability of two well-formed JSON files in a directory named `slate_sync_vars` in the top level of your HOME directory (this is generally the directory corresponding to your user on your local machine). The files and their formats, wherein each capscase word in curly braces should be substituted with your unique values, are:

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

Currently, the SQL Server `{DRIVERNAME}` is `{ODBC Driver 13 for SQL Server}` (the braces here are literal and should be included in the file).

```qvars.json```

```
{
  "oracle": {
    "termlb": "{TERMLOWERBOUND}",
    "termub": "{TERMUPPERBOUND}"
  }
}
```
