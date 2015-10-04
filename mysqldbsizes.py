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
            'host': 'hostname or IP address of MySQL server to collect from.' +
            'This is the only required argument. If you omit any of the ' +
            'non-required arguments they will be copied from the root section ' +
            'if defined.',
            'db': 'Database to connect to if not specified in hosts. ' +
            'Use db "None" to avoid connecting to a particular db. ' +
            'By default the collector uses `INFORMATION_SCHEMA`',
            'user': 'User name for authenticating to the MySQL database. ',
            'password': 'Password for authenticating to the MySQL database',
            'port': 'Port number. If not specified in hosts. By default ' +
            '3306 will be used.',
            'connection_timeout': 'Specify the connection timeout. Set to ' +
            '30 seconds by default. The collector will abort connections if ' +
            'they are not established within the timeout.',
            'ssl': 'Currently not implemented and ignored. False by default. ' +
            'To enable SSL connections to the MySQL server(s) you need to have ' +
            'a set of certificate and private key readable by diamond.'
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
            'port': 3306,
            'db': 'information_schema',
            'user': '',
            'password': '',
            'ssl': False,
            'connection_timeout': 30,
        })
        return config

    def get_db_results(self, query):
        cursor = self.db.cursor(cursorclass=MySQLdb.cursors.DictCursor)

        try:
            cursor.execute(query)
        except (AttributeError, MySQLdb.OperationalError), e:
            self.log.error('%s: got an error "%s" executing query: "%s"', self.name, e, query)
            raise
        # rows = cursor.fetchall()
        return cursor.fetchall()

    def connect(self, params):
        try:
            self.db = MySQLdb.connect(**params)
        except MySQLdb.Error, e:
            self.log.error('%s: could not connect to database %s', self.name, e)
            raise
        self.log.debug('%s: connected to database %s@%s:%s', self.name, params['user'], params['host'], params['port'])
        return True

    def get_sizes(self, params):
        metrics = {}

        if not self.connect(params):
            return metrics

        self.log.debug('%s: getting table sizes from database', self.name)
        try:
            rows = self.get_db_results("""
                SELECT
                    table_schema, table_name, table_rows,
                    data_length, index_length, data_free
                FROM INFORMATION_SCHEMA.TABLES
                WHERE
                table_type='BASE TABLE'
                AND table_schema NOT IN ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','mysql')
            """)
        except (AttributeError, MySQLError), e:
            self.log.error('%s: could not get table sizes: %s', self.name, e)
            raise

        for row in rows:
            metric_name=row['table_schema'] + "." + row['table_name']
            self.log.debug('%s: found metrics for: %s', self.name, metric_name)
            metrics[metric_name] = row
        return metrics

    def get_conn_params(self, config):
        params = {
                    'host': config['host'],
                    'user': config['user'],
                    'passwd': config['password'],
                    }
        # convert connection_timeout to integer
        if config['connection_timeout']:
            params['connect_timeout'] = int(config['connection_timeout'])
        try:
            params['port'] = int(config['port'])
        except (ValueError, TypeError):
            params['port'] = config['port']

        if config['db']:
            params['db'] = config['db']
        # TODO: fix ssl configuration
        params['ssl'] = False

        return params

    def disconnect(self):
        self.db.close()

    def copymissing(self, left, right):
        for key, val in list(left.items()):
            if key in right or isinstance(val, dict):
                continue
            else:
                right[key] = val

    def process_config(self):
        super(MySQLSizeCollector, self).process_config()

        # get a list of sections (Section objects)
        sections = self.config.sections

        # if no default section is specified in the configuration file, use the
        # root ConfigObj object as 'default' to fill any missing parameters.

        for section in sections:
            if not self.config[section].get('host'):
                self.log.warn('%s: config section %s has no host parameter defined, skipping', self.name, section)
                # skip sections without a host
                continue

            # set host alias to section title for sections that have a host but no alias
            if 'alias' not in self.config[section]:
                self.config[section]['alias'] = re.sub('[:\. /]', '_', section)
            else:
                # sanitize aliases/section names
                self.config[section]['alias'] = re.sub('[:\. /]', '_', self.config[section]['alias'])

            # copy all missing configuration from the root config object, without overwriting
            self.copymissing(cfg, self.config[section])

        # set an alias for the root section last, so it doesn't get copied to all other sections
        if not ('alias' in self.config and 'default' in sections):
            self.config['alias'] = 'default'

    def collect(self):

        if MySQLdb is None:
            self.log.error('%s: unable to import MySQLdb', self.name)
            return False

        conn_params = {}
        metrics = {}
        if 'host' in self.config:
            conn_params[self.config['alias']] = self.get_conn_params(self.config)

        for section in self.config.sections:
            # skip sections without a defined host
            if 'host' not in self.config[section]:
                continue

            conn_params[self.config[section]['alias']]=self.get_conn_params(self.config[section])

        for alias in conn_params:
            try:
                metrics[alias] = self.get_sizes(params=conn_params[alias])
            except MySQLdb.OperationalError, e:
                self.log.error('%s: collection failed for %s: %s, skipping', self.name, alias, e)
                continue
            except Exception, e:
                self.log.error('%s: collection failed for %s: %s', self.name, alias, e)
                raise
            finally:
                try:
                    self.disconnect()
                except AttributeError:
                    # failed to disconnect from the database, most probably never connected
                    pass
                except MySQLdb.ProgrammingError:
                    self.log.error('%s: programming error: %s', self.name)
                    pass


        for alias in metrics:
            for metric in metrics[alias].keys():
                self.log.debug('%s: publishing metrics for host: %s: %s', self.name, alias, metric)
                for key, value in metrics[alias][metric].items():
                    if key in ('table_schema','table_name'):
                        continue
                    self.publish(metric + "." + key, value)
