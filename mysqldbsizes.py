# coding=utf-8

"""

Diamond collector that monitors MySQL table sizes 

#### Dependencies

 * MySQLdb
 * MySQL 5.0.3+

"""


try:
    import MySQLdb
    from MySQLdb import MySQLError
except ImportError:
    MySQLdb = None
import diamond

class MySQLSizeCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        """
        Return help text for collector
        """
        config_help = super(MySQLSizeCollector, self).get_default_config_help()
        config_help.update({
            'host': 'A single hostname[:port] to get metrics from',
            'db': '(Optional) database to connect to',
            'user': 'Username for authenticating to the RDBMS',
            'password': 'Password for authenticating to the RDBMS',
            'port': 'Port number',
            'ssl': 'True to enable SSL connections to the MySQL server.'
                    ' Default is False',
            'databases': 'list of databases to collect sizes',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MySQLSizeCollector, self).get_default_config()
        config.update({
            'path':     'mysql',
            # Connection settings
            'host': 'localhost',
            'port': 3306,
            'db': 'information_schema',
            'user': 'stat',
            'password': '',
            'ssl': False,

        })
        return config

    def get_db_results(self, query):
        import pdb
#        pdb.set_trace()
        cursor = self.db.cursor(cursorclass=MySQLdb.cursors.DictCursor)

        try:
            cursor.execute(query)
        except (AttributeError, MySQLdb.OperationalError), e:
            self.log.error('%s: got an error "%s" executing query: "%s"', self.__class__.__name__, e, query)
            raise e
        # rows = cursor.fetchall()
        return cursor.fetchall()

    def connect(self, params):
        try:
            self.db = MySQLdb.connect(**params)
            self.log.debug('%s: connected to database %s@%s:%s/%s', self.__class__.__name__, params['user'], params['host'], params['port'], params['db'])
        except MySQLError, e:
            self.log.error('%s: could not connect to database %s', self.__class__.__name__, e)
            return False
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
            raise e 
        return metrics

    def disconnect(self):
        self.db.close()

    def collect(self):

        if MySQLdb is None:
            self.log.error('%s: unable to import MySQLdb', self.__class__.__name__)
            return False

        conn_params = {
            'host': self.config['host'],
            'user': self.config['user'],
            'passwd': self.config['password'],
            'db': self.config['db'],
            'port': self.config['port'],
            'ssl': self.config['ssl'],
        }

        try:
            metrics = self.get_sizes(params=conn_params)
        except Exception, e:
            try:
                self.disconnect()
            except MySQLdb.ProgrammingError:
                pass
            self.log.error('%s: collection failed for %s %s', self.__class__.__name__, conn_params['host'], e)
            raise e


        for name in metrics:
            self.log.debug('%s: publishing metrics for %s', self.__class__.__name__, name)
            for metric, value in metrics[name].items():
                if metric in ('table_schema','table_name'):
                    continue 
                self.log.debug('%s: publish for %s: %s=%s', self.__class__.__name__, name, metric, value)
                self.publish(name + "." + metric, value)
