from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

class Reader(object):
    def __init__(self, spark):
        """

        :type spark: pyspark.sql.SparkSession
        """
        sc = spark.sparkContext
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self._spark = spark

    def build_schema(self, metadata, fileconfig):
        data_types = metadata.data_types

        derived_columns = fileconfig.get('derived_columns')
        if derived_columns:
            col_num = len(data_types) - len(derived_columns)
        else:
            col_num = len(data_types)

        columns_to_drop = fileconfig.get('columns_to_drop')
        if columns_to_drop:
            col_num = col_num + len(columns_to_drop.split(','))

        schema = StructType()

        for i in range(col_num):
            schema.add(StructField("_c" + str(i), StringType(), True))

        self.logger.info('schema=%s' % schema.simpleString())

        return schema

    def add_derived_columns(self, df, fileconfig, process_info):
        file_name = process_info.get('file_name')

        if fileconfig.get('derived_columns'):
            column_num = len(df.columns)
            for column in fileconfig.get('derived_columns'):
                colum_name = column.get('column_name')
                derive_functions = column.get('derive_functions')
                self.logger.info("adding derive column %s using function %s" % (colum_name, derive_functions))
                df = df.withColumn('_c' + str(column_num), eval(derive_functions))
                column_num = column_num + 1

        return df

    def filter(self, df, fileconfig):
        """

        :type df: spark.sql.DataFrame
        :type fileconfig: {}
        """
        df = df.dropna(how='all')

        filterstr = fileconfig.get('row_filter_condition')

        if filterstr:
            self.logger.info('filtering condition is: ' + filterstr)
            df = df.filter(filterstr)

        return df

    def drop_columns(self, df, fileconfig):
        columns_to_drop_str = fileconfig.get('columns_to_drop')
        self.logger.info("dropping columns %s" % columns_to_drop_str)
        column_list = columns_to_drop_str.split(',')
        self.logger.debug("schema before dropping columns is %s" % df.schema)
        df = df.drop(*column_list)
        self.logger.debug("schema after dropping columns is %s" % df.schema)
        return df

    def read_excel(self, filepath, file_config, metadata, process_info):
        dfReader = self._spark.read.format("com.crealytics.spark.excel").option("useHeader", "false")

        dataAddressStr = file_config.get('data_address')
        if dataAddressStr:
            self.logger.info('read data from data address %s' % dataAddressStr)
            dfReader = dfReader.option("dataAddress", dataAddressStr)

        if file_config.get('read_with_schema', "no") == "yes":
            schema = self.build_schema(metadata, file_config)
            dfReader = dfReader.schema(schema)

        df = dfReader.load(filepath)

        return df

    def read_delimited(self, filepath, file_config, metadata, process_info):
        self.logger.info('reading delimited file %s' % filepath)
        delimiter = file_config.get('delimiter', ',')

        if file_config.get('read_with_schema', "no") == "yes":
            schema = self.build_schema(metadata, file_config)
            df = self._spark.read.csv(filepath, schema=schema, sep=delimiter)
        else:
            df = self._spark.read.csv(filepath, sep=delimiter)

        return df

    def read_positional(self, filepath, file_config, metadata, process_info):
        df = self._spark.read.text(filepath)

        spark_table = 'spark_' + metadata.table_name + '_' + process_info.get('file_no')
        df.createOrReplaceTempView(spark_table)

        sqlstr = 'select '
        col_num = 1
        cur_pos = 1
        for col_length_str in metadata.col_lengths:
            if col_num > 1:
                sqlstr = sqlstr + ', '

            col_length = int(col_length_str[col_length_str.rfind('=')+1:])
            sqlstr = sqlstr + 'substr(value, {cur_pos}, {length}) as _c{col_num}'.format(cur_pos=cur_pos, length=col_length, col_num=col_num)

            cur_pos = cur_pos + col_length
            col_num = col_num + 1

        sqlstr = sqlstr + ' from {spark_table}'.format(spark_table=spark_table)

        self.logger.info(sqlstr)
        df = self._spark.sql(sqlstr)
        return df

    def read(self, filepath, file_config, metadata, process_info):
        file_type = file_config.get('file_type')

        if file_type == 'excel':
            df = self.read_excel(filepath, file_config, metadata, process_info)
        elif file_type == 'delimited':
            df = self.read_delimited(filepath, file_config, metadata, process_info)
        else:
            df = self.read_positional(filepath, file_config, metadata, process_info)

        self.logger.debug(df.first())
        #df.cache()
        df = self.add_derived_columns(df, file_config, process_info)
        df = self.filter(df, file_config)

        if file_config.get('columns_to_drop'):
            df = self.drop_columns(df, file_config)

        df.cache()

        return df

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from metadata import Metadata
    import config

    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("ingestion") \
        .master("local[2]") \
        .getOrCreate()

    sc = spark.sparkContext

    config = config.Config(sc, "../conf/config_ta_asia_test.json")

    metadata = Metadata(sc)
    metadata.loadKVFile(spark, config, "metadata_etf_transactions.kv")
    process_info = {}
    reader = Reader(spark)
    file = '/test/IIC_MPower_EntriesJul22-19.xls'

    file_config = config.get('files')[3]

    print(config.get('source'))
    print(file_config.get('topic'))

    df = reader.read(file, file_config, metadata, process_info)

    print(df.count())
    print(df.show(1000))
