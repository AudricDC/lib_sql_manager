# Lib Sql Manager

One library to connect to different SQL server, and make some basic requests using python.


## Requirements

See requirements.txt


## Development setup

This script uses a file named database.ini. This file should look like this:

```
[postgresql]
user=user_name 
password=one_secret_password
query= An SQL request
; for example : query=SELECT * FROM {my_table}
```

Note: default user is postgres and default port is 5432 (see code).\
Note2: The query must be somtehing that looks like SELECT ... FROM {my_table}. To specify a table name, 
you have to do it in collectDf and collectWithPsycopg2 methods.


## Script description

sql.py contains an Sql class that does the work.\
Sql reads a database.ini to load attributes from the file.\
Also contains collectWithPsycopg2, _initConnection, collectDf and dfToSql methods. See script for function descritpions.
