
class Metadata(object):
    def __init__(self, sc):
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self.col_names = []
        self.data_types = []
        self.col_lengths = []
        self.table_name = ''
        self._sc = sc

    def loadKVFile(self, spark, config, kv_file, process_info):
        source = config.get('source')
        sub_source = config.get('sub_source')
        hdfs_registry_base_folder = config.get('hdfs_registry_base_folder')

        if sub_source:
            hdfs_registry_folder = hdfs_registry_base_folder + '/' + source + '/' + sub_source
        else:
            hdfs_registry_folder = hdfs_registry_base_folder + '/' + source

        kv_file_path = hdfs_registry_folder + '/' + kv_file

        spark_pool = 'pool' + process_info['file_no']
        self._sc.setLocalProperty("spark.scheduler.pool", spark_pool)
        kvrows = spark.read.text(kv_file_path, wholetext=True).collect()
        kvlist = kvrows[0].value.replace('\r', '').split('\n')

        self.col_names = [item for item in kvlist if item[:12] == 'column_name_']
        self.data_types = [item for item in kvlist if item[:10] == 'data_type_']
        self.col_lengths = [item for item in kvlist if item[:7] == 'length_']
        table_name_str = [item for item in kvlist if 'table_name' in item][0]
        self.table_name = table_name_str[table_name_str.rfind('=')+1:]

        if len(self.table_name) > 0 and len(self.data_types) > 0:
            self.logger.info('kv file ' + kv_file_path + ' is loaded')
        else:
            self.logger.error('kv file ' + kv_file_path + ' is not loaded')


    def log_registry_table(self, spark, file_config, config, process_info):
        sqlstr = """select \'{source}\' as source,\'{topic}\' as topic,\'{filename}\' as filename,
               \'{user_name}\' as user_name,\'{hdfs_location}\' as hdfs_location,
               to_timestamp(\'{process_timestamp}\', \'yyyy-MM-dd HH:mm:ss\') as process_timestamp,
               {record_count} as record_count,\'{status}\' as status,
               to_timestamp(\'{process_endtime}\', \'yyyy-MM-dd HH:mm:ss\') as process_endtime,\'{table_name}\' as table_name
        """.format(source=config.get('source'),
                   topic=file_config.get('topic'),
                   filename=process_info.get('file_name'),
                   user_name=process_info.get('user_name'),
                   hdfs_location=process_info.get('hdfs_datafile_path'),
                   process_timestamp=process_info.get('process_start_timestamp'),
                   record_count=process_info.get('row_count'),
                   status='SUCCESS',
                   process_endtime=process_info.get('process_end_timestamp'),
                   table_name=self.table_name)

        self.logger.info(sqlstr)
        data = spark.sql(sqlstr)

        if config.get('registryDB'):
            dbname = config.get('registryDB')
        else:
            dbname = config.get('typedDB')

        data.write.insertInto(dbname + '.' + config.get('registry_table'))

    def log_error_table(self, spark, file_config, config, process_info, msg):
        sqlstr = """select 'ERROR' as type,
                    \'spark_ingestion' as process_group,
                    \'batch_file_ingestion\' as process_name,
                    \'{message}\' as message,
                    \'{source}\' as source,
                    \'{topic}\' as topic,
                    \'{filename}\' as filename,
                    \'{user_name}\' as user_name,
                    \'\' as comments,
                    to_timestamp(\'{process_timestamp}\', \'yyyy-MM-dd HH:mm:ss\') as process_timestamp,
                    to_timestamp(\'{process_timestamp}\', \'yyyy-MM-dd HH:mm:ss\') as error_timestamp
        """.format(message=msg,
                   source=config.get('source'),
                   topic=file_config.get('topic'),
                   filename=process_info.get('file_name'),
                   user_name=process_info.get('user_name'),
                   process_timestamp=process_info.get('process_start_timestamp'))

        self.logger.info(sqlstr)
        data = spark.sql(sqlstr)

        if config.get('registryDB'):
            dbname = config.get('registryDB')
        else:
            dbname = config.get('typedDB')

        data.write.insertInto(dbname + '.' + config.get('error_log_table'))

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    import config

    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName('ingestion') \
        .getOrCreate()

    sc = spark.sparkContext
    config = config.Config(sc, "../conf/config_ta_asia_hk.json")
    file_config = config.get('files')[3]

    metadata = Metadata(sc)
    kv_file_path = config.get('hdfs_registry_base_folder') + '/' + file_config.get('kv_file')
    metadata.loadKVFile(spark, kv_file_path)

    print(metadata.table_name)

    for length in metadata.col_lengths:
        print(length)
