
class Transformer(object):
    def __init__(self, sc):
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")

    def gen_transform_sql(self, columns, file_config, metadata, process_info):
        """

        :type columns: List[String]
        """
        spark_table = 'spark_' + metadata.table_name + '_' + process_info.get('file_no')
        datatypelist = metadata.data_types
        sqlstr = 'select '
        i = 0

        for column in columns:
            if i > 0:
                sqlstr = sqlstr + ', '

            sqlstr = sqlstr + 'trim(' + column + ') as ' + column
            datatypestr = datatypelist[i]
            datatype = datatypestr[datatypestr.rfind('=')+1:].strip()

            if datatype.lower().startswith('decimal'):
                sqlstr = sqlstr + ', cast(translate(translate(trim(' + column + '), \'()\', \'-\'), \'\",$\', \'\') as ' + datatype + ') as _' + column
            elif datatype.lower().startswith('date'):
                if datatype.rfind('(') > 0:
                    dateformat = datatype[datatype.rfind('(')+1 : datatype.rfind(')')]
                else:
                    dateformat = file_config.get('default_date_format', 'yyyy-MM-dd')

                sqlstr = sqlstr + ', to_date(trim(' + column + '), \'' + dateformat + '\') as _' + column
            elif datatype.lower().startswith('timestamp'):
                if datatype.rfind('(') > 0:
                    timeformat = datatype[datatype.rfind('(')+1 : datatype.rfind(')')]
                else:
                    timeformat = file_config.get('default_timestamp_format', 'yyyy-MM-dd HH:mm:ss')

                sqlstr = sqlstr + ', to_timestamp(trim(' + column + '), \'' + timeformat + '\') as _' + column
            elif datatype.lower().find('int') >= 0:
                sqlstr = sqlstr + ', cast(trim(' + column + ') as ' + datatype + ') as _' + column

            i = i + 1

        sqlstr = sqlstr + ' from ' + spark_table
        return sqlstr

    def trans_data_types(self, spark, df, file_config, metadata, process_info):
        spark_table = 'spark_' + metadata.table_name + '_' + process_info.get('file_no')
        df.createOrReplaceTempView(spark_table)
        transformSQL = self.gen_transform_sql(df.columns, file_config, metadata, process_info)
        self.logger.info(transformSQL)
        data = spark.sql(transformSQL)
        return data
