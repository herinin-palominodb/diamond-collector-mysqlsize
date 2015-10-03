# coding=utf-8

"""

Diamond collector that monitors MySQL table sizes

#### Dependencies

 * MySQLdb
 * MySQL 5.0.3+

### Grants and Privileges

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

You can put a section named `[[MySQLSizeCollector]]` under `[collectors]` in
your diamond.conf:

    [collectors]
    [[MySQLSizeCollector]]
    # enable the collector
    enabled = True
    # no need to gather statistics more frequently
    interval = 600


Diamond collectors that require a separate configuration file should place a
.conf file in /etc/diamond/collectors/.
The configuration file name should match the name of the diamond collector
class. Configuration file for this collector should be named `MySQLSizeCollector.conf`
/etc/diamond/collectors/MySQLSizeCollector.conf.

Example configuration file:

    hosts = [ stat1:rndpass@host1:3308/None/db_server1, host2:3307/information_schema/db_server2, host ]
    user = anotherstat
    password = somerandompassword


"""


try:
    import MySQLdb
    from MySQLdb import MySQLError
except ImportError:
    MySQLdb = None

import diamond
import re

class MySQLSizeCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        """
        Return help text for collector
        """
        config_help = super(MySQLSizeCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'List of hosts to collect from. Format is ' +
            '[username:yourpassword@]host[:port][/db][/nickname]' +
            'use db "None" to avoid connecting to a particular db',
            'user': 'Username for authenticating to the RDBMS',
            'password': 'Password for authenticating to the RDBMS',
            'port': 'Port number',
            'connection_timeout': 'Specify the connection timeout',
            'ssl': 'True to enable SSL connections to the MySQL server(s).'
                    ' Default is False. This is option is currently unimplemented.',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MySQLSizeCollector, self).get_default_config()
        config.update({
            'path':     'mysql.size',
            # Connection settings
            'hosts': 'localhost:3306/information_schema`',
            'port': 3306,
            'db': 'information_schema',
            'user': '',
            'password': '',
            'ssl': False,
            'connection_timeout': 60,
        })
        return config

    def get_db_results(self, query):
        cursor = self.db.cursor(cursorclass=MySQLdb.cursors.DictCursor)

        try:
            cursor.execute(query)
        except (AttributeError, MySQLdb.OperationalError), e:
            self.log.error('%s: got an error "%s" executing query: "%s"', self.__class__.__name__, e, query)
            raise
        # rows = cursor.fetchall()
        return cursor.fetchall()

    def connect(self, params):
        try:
            self.db = MySQLdb.connect(**params)
        except MySQLdb.Error, e:
            self.log.error('%s: could not connect to database %s', self.__class__.__name__, e)
            raise
        self.log.debug('%s: connected to database %s@%s:%s', self.__class__.__name__, params['user'], params['host'], params['port'])
        return True

    def get_sizes(self, params):
        metrics = {}

        if not self.connect(params):
            return metrics

        self.log.debug('%s: getting table sizes from database', self.__class__.__name__)
        rows = self.get_db_results("""
            SELECT
                table_schema, table_name, table_rows,
                data_length, index_length, data_free
            FROM INFORMATION_SCHEMA.TABLES
            WHERE
                table_type='BASE TABLE'
                AND table_schema NOT IN ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','mysql')
        """)

        try:
            for row in rows:
                metric_name=row['table_schema'] + "." + row['table_name']
                self.log.debug('%s: processing: %s', self.__class__.__name__, metric_name)
                metrics[metric_name] = row
        except (AttributeError, MySQLError), e:
            self.log.error('%s: could not get table sizes: %s', self.__class__.__name__, e)
            raise
        return metrics

    def disconnect(self):
        self.db.close()

    def parsehoststr(self, hoststr):
        params = {}
        self.log.debug('%s: parsing hoststr: %s', self.__class__.__name__, hoststr)
        # matches = re.search('\A(?:([^:]*)(?::([^@]*))?@)?([^:/]*)(?:(?::([^/]*))?/?([^/]*)?/?(.*))?\Z', hoststr)
        matches = re.match('\A(?:([^:]*)(?::([^@]*))?@)?([a-zA-Z0-9_\.-]+)(?:(?::([^/]*))?(?:/([^/]*)?)?(?:/(.*))?)?\Z', hoststr)

        if not matches:
            raise ValueError('string ' + hoststr + ' is not in the expected format')

        params['host'] = matches.group(3)

        if matches.group(1):
            params['user'] = matches.group(1)
        else:
            params['user'] = self.config.get('user')

        if matches.group(2):
            params['passwd'] = matches.group(2)
        else:
            params['passwd'] = self.config.get('password')

        try:
            params['port'] = int(matches.group(4))
        except (ValueError, TypeError):
            params['port'] = self.config.get('port')

        if matches.group(5):
            params['db'] = matches.group(5)
        else:
            params['db'] = self.config.get('db')

        if params['db'] == 'None':
            del params['db']

        if matches.group(6):
            params['alias'] = matches.group(6)
        elif len(self.config.get('hosts')) == 1:
            # one host only, no need for an alias
            del params['alias']
        else:
            params['alias'] = re.sub('[:\.]', '_', params['host'] + ":" + str(params['port']))
        self.log.debug('%s: params: %s', self.__class__.__name__, params)
        return params

    def collect(self):

        if MySQLdb is None:
            self.log.error('%s: unable to import MySQLdb', self.__class__.__name__)
            return False

        hosts = self.config.get('hosts')

        # Convert a string config value to be an array
        if isinstance(hosts, basestring):
            hosts = [hosts]

        # convert connection_timeout to integer
        if self.config['connection_timeout']:
            self.config['connection_timeout'] = int(self.config['connection_timeout'])

        for host in hosts:
            try:
                conn_params = self.parsehoststr(hoststr=host)
            except ValueError as e:
                self.log.warn('%s: Connection string parsing failed: %s, skipping host', self.__class__.__name__, e)
                continue

            if 'alias' in conn_params:
                del conn_params['alias']

            conn_params['connect_timeout'] = self.config['connection_timeout']

            try:
                metrics = self.get_sizes(params=conn_params)
            except MySQLdb.OperationalError, e:
                self.log.error('%s: collection failed for %s: %s, skipping', self.__class__.__name__, host, e)
                continue
            except Exception, e:
                self.log.error('%s: collection failed for %s: %s', self.__class__.__name__, conn_params['host'], e)
                raise
            finally:
                try:
                    self.disconnect()
                except AttributeError:
                    # failed to disconnect from the database, most probably never connected
                    pass
                except MySQLdb.ProgrammingError:
                    self.log.error('%s: programming error: %s', self.__class__.__name__)
                    pass


            for name in metrics.keys():
                self.log.debug('%s: publishing metrics for %s', self.__class__.__name__, name)
                for metric, value in metrics[name].items():
                    if metric in ('table_schema','table_name'):
                        continue
                    self.log.debug('%s: publish for %s: %s=%s', self.__class__.__name__, name, metric, value)
                    self.publish(name + "." + metric, value)
