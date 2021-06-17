# MySQL_copy_table

To run this script you need python version 3.5 or newer and MySQL connector module, to install it run command:
>$ pip install mysql-connector-python

To run script run command:
>$ python copy_table.py source_config.txt target_config.txt

Files source_config.txt and target_config.txt contain information necessary to connect to databases.
- source_config.txt contains information about database FROM which program takes data.
- target_config.txt contains information about database TO which program copies data.

These files contain information in format:

Argument | Description
----- | -----
user=username              | The user name used to authenticate with the MySQL server.
password=password          | The password to authenticate the user with the MySQL server.
host=127.0.0.1             | The host name or IP address of the MySQL server
port=3306                  | The TCP/IP port of the MySQL server. Must be an integer.
database=database_name     | The database name to use when connecting with the MySQL server.

Change values after "=" in those files to your own.

DO NOT add additional lines to those files and/or change text before "=" sign, it might cause program to crash.
Make sure that target database does not have duplicate entries in the "titles" table,
if program detects duplicate data it terminates.