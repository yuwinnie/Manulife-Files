from datetime import datetime


class Writer(object):
    def __init__(self, sc):
        """

        :type sc: SparkContext
        """
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self._sc = sc

    def add_partition(self, spark, config, metadata, current_date, filepath):
        sqlstr = 'ALTER TABLE {dbname}.{tablename} ADD IF NOT EXISTS PARTITION(process_date=\'{process_date}\') LOCATION \'{filepath}\'' \
            .format(dbname=config.get('typedDB'), tablename=metadata.table_name, process_date=current_date, filepath=filepath)

        self.logger.info('sqlstr = ' + sqlstr)
        spark.sql(sqlstr)

    def analyze_table(self, spark, config, metadata, current_date):
        sqlstr = 'ANALYZE TABLE {dbname}.{tablename} PARTITION(process_date=\'{process_date}\') COMPUTE STATISTICS' \
            .format(dbname=config.get('typedDB'), tablename=metadata.table_name, process_date=current_date)

        self.logger.info('sqlstr = ' + sqlstr)
        spark.sql(sqlstr)

    def write_errorfile(self, df, config, fileconfig, process_info):
        hdfs_error_archive_base_folder = config.get('hdfs_error_archive_base_folder')
        source = config.get('source')
        sub_source = config.get('sub_source')
        topic = fileconfig.get('topic')

        if sub_source:
            hdfs_error_archive_folder = hdfs_error_archive_base_folder + '/' + source + '/' + sub_source + '/' + topic
        else:
            hdfs_error_archive_folder = hdfs_error_archive_base_folder + '/' + source + '/' + topic

        #current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = process_info['file_name']

        filepath = hdfs_error_archive_folder + '/' + filename + '.err_' + self._sc.applicationId
        df.coalesce(1).write.csv(filepath, mode='Overwrite')
        process_info['error_file_path'] = filepath
        self.logger.info('error_file_path = ' + filepath)

    def write_orc(self, df, spark, config, metadata, process_info):
        current_date = process_info['process_start_timestamp'][:10]
        filepath = config.get('hdfs_data_base_folder') + '/' + metadata.table_name + '/process_date=' + current_date
        self.logger.info('writing orc file to path %s' % filepath)
        num_partitions = process_info['row_count']//200000 + 1
        #df.coalesce(num_partitions).write.orc(filepath, mode="append", compression="zlib")
        #df.write.format("orc").save(filepath, mode="append", compression="zlib")
        df.coalesce(num_partitions).write.format("orc").save(filepath, mode="append")
        process_info['hdfs_datafile_path'] = filepath
        self.logger.info('file written sucessfully to temp path. orc_file_path = ' + filepath)

        self.add_partition(spark, config, metadata, current_date, filepath)
        #self.analyze_table(spark, config, metadata, current_date)

