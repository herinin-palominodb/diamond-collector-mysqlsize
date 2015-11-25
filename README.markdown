
Diamond collector that monitors MySQL table sizes

#### Dependencies

 * MySQLdb
 * MySQL 5.0.3+

#### Considerations

The information about table sizes is obtained from INFORMATION_SCHEMA.TABLES.
As such it is not guaranteed to be 100% accurate, but is subject to the
frequency with which the InnoDB and other engines update this information.

Additional concern is the performance impact of running queries against
INFORMATION_SCHEMA.TABLES. By default MySQL tries to update table statistics
on each access to certain tables from INFORMATION_SCHEMA. This causes delays
and needlessly increases the load on the server. In order to stop MySQL from
invalidating statistics on every INFORMATION_SCHEMA query you need to set
*`innodb_stats_on_metadata = 0`* in your MySQL configuration. You can also set
this at runtime because it is a global dynamic variable.

#### Grants and Privileges

MySQL filters the information visible from `information_schema` tables based
on the access privileges of the user executing the query. Information about
database objects that the user is not permitted to access is hidden. To get
access to information about all tables from `information_schema` the MySQL
user needs at least `REFERENCES` privilege on the database (`*.*`).

In versions of MySQL before 5.5.41, 5.6.22, and 5.7.6, the `REFERENCES`
privilege is marked as 'unused'. After these versions the privilege gives
additional access. If `REFERENCES` privilege is unavailable you will need to
grant access (`SELECT`) to the databases and tables.

Example:

    -- Create user for statistics gathering:
    CREATE USER `stats`@`localhost` IDENTIFIED BY `somerandompassword`;
    -- Grant access to information about tables and databases from information_schema schema
    GRANT REFERENCES ON *.* TO 'stats'@'localhost';

    -- Grant SELECT on all tables from specific databases
    -- GRANT SELECT on database_prod1.* to `stats`@`localhost`;
    -- GRANT SELECT on database_prod2.* to `stats`@`localhost`;

#### Customizing

Diamond looks for collectors in /usr/lib/diamond/collectors/ (on Ubuntu). By
default diamond will invoke the *collect* method every 60 seconds.

There are several ways to configure a python-diamond collector.

The simplest way if you have a single host is to put all the
configuration in your `diamond.conf` in a section named
`[[MySQLSizeCollector]]` under [collectors].

    [collectors]
    [[MySQLSizeCollector]]
    # enable the collector
    enabled = True
    # no need to gather statistics more frequently
    interval = 600
    host = localhost
    user = stats
    password = stats

The only required configuration parameter is `host`. All other parameters will be
assigned default values if unspecified.

Another way to configure Diamond collectors is to use a separate configuration
file placed in `/etc/diamond/collectors/`. The configuration file name should
match the name of the python-diamond collector class. The configuration file
for this collector should be named `MySQLSizeCollector.conf`
(`/etc/diamond/collectors/MySQLSizeCollector.conf`).

In this configuration file you can specify the connection information of
your MySQL server. If you don't specify a non-required parameter (everything but
"host") it will be assigned a default value. Here is a sample config file:

    host = localhost
    user = stats
    password = stats
    db = prod1

In this case MySQLSizeCollector will connect to host localhost with user 'stats'
and password 'stats', database 'prod1' with a default connection_timeout of 30
seconds.

If you want to specify the data from more than one MySQL server to be collected,
you should put each host in a separate section in your config file. A section is
created by putting a string (optionally quoted, if it contains spaces or other
non-standart hostname characters) on a new line in square brackets
(eg. `[server1]`, `[prod-server-sydney]`).

Whatever parameters are left unspecified in each section will be set to the corresponding
parameters from the root section (top of your config file, without a section) of the
config file, if specified, or the built-in default values.

Example configuration file:

    user = anotherstat
    password = somerandompassword

    [master-prod]
    host = server1.prod.example.com

    [replica1-prod]
    host = mysql-slave.prod.example.com
    db = prod
    port = 3309

    [replica2-prod]
    host = mysql-slave.sydney.example.com
    db = drprod
    # this is a host running at the other end of the world, give it more time to respond
    connection_timeout = 60
    user = drstat

In this particular example the collector will try to obtain table sizes from
'server1.prod.example.com' using username:password 'anotherstat:somerandompassword. The db
parameter for this host is not specified, so the connection will be established to
`information_schema` schema, on port 3306 and the connection_timeout will be the default
of 30 seconds.
The collector will also connect to 'mysql-slave.prod.example.com' on port 3309, database
'prod' using username and password supplied in the root section of the config file
('anotherstat', 'somerandompassword'). Connections to 'mysql-slave.sydney.example.com' host will
use port 3306, database 'drprod' and user 'drstat'. The password will be the one supplied
in the root configuration section ('somerandompassword').

SSL connections are not implemented yet.

#### Metrics

By default metrics are collected from each host specified in the configuration file. The
metrics that are published are:
1. table_rows
2. data_length
3. index_length
4. data_free
If you don't want to publish any of them you can use the standard configuration parameters
`[ metrics_whitelist ]` and `[ metrics_blacklist ]` to list to blacklist or whitelist
individual metrics.

##### Naming
By default all metrics are published using the following hierarchy:

    servers.<hostname>.mysql.[alias.]size.<schema>.<table>.<metric>

If you have only one host specified in your configuration file then the `[alias]` part of
the path will be empty. This allows for simpler classification of metrics if you don't
need to specify more than one host. If there are sections that specify more hosts, then
alias will be either the name of the section or the value of the `alias` configuration 
option, if supplied.

Each alias is sanitized by replacing ":", ".", "/", and "<space>" with "_" (underscore).

