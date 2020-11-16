# Lib Sql Retriever

One library to connect to different SQL server, and make some basic requests using python.


## Requirements

See requirements.txt


## Development setup

This script uses a file named database.ini. This file should look like this:

```[postgresql]
host=your_host_name
database=db_name
user=user_name 
password=one_secret_password
query= An SQL request
; for example : SELECT * FROM my_table
```

Note: default user is postgres and default port is 5432.

## Script description

sql.py contains an Sql class that does the work.\
Sql reads a database.ini to load attributes from the file.\
Also contains collectWithPsycopg2, _initConnection, collectDf and dfToSql methods. See script for function descritpions.
