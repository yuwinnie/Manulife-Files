
class Validator(object):
    def __init__(self, sc):
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self._err_msg = None

    def gen_validation_sql(self, valcolumns, file_config, metadata, process_info):
        spark_table = 'spark_' + metadata.table_name + '_' + process_info.get('file_no')
        sqlstr = 'select *'

        if len(valcolumns) > 0:
            sqlstr = sqlstr + ', concat('

            i = 0
            for column in valcolumns:
                fieldnum = int(column[3:])

                if file_config.get("columns_to_drop"):
                    fieldnum = fieldnum - len(file_config.get("columns_to_drop").split(','))

                datatypestr = metadata.data_types[fieldnum]
                datatype = datatypestr[datatypestr.rfind('=') + 1:].strip()

                if i > 0:
                    sqlstr = sqlstr + ', '

                sqlstr = sqlstr + 'if(' + column + ' is null and ' + column[1:] + ' is not null and trim(' + \
                         column[1:] + ') != \'\'' \

                null_values = file_config.get('null_values')
                if null_values:
                    sqlstr = sqlstr + ' and not array_contains(array(' + null_values + '), trim(' + column[1:] + '))'

                sqlstr = sqlstr + ', concat(' + \
                         column[1:] + ', \' is not a valid value for ' + datatype + ' in column ' + \
                         str(fieldnum+1) + '\', \'|\'), \'\')'

                i = i + 1
            sqlstr = sqlstr + ') as _error_message'
        else:
            sqlstr = sqlstr + ', NULL as _error_message'

        sqlstr = sqlstr + ' from ' + spark_table
        return sqlstr

    def val_data_types(self, spark, df, file_config, metadata, process_info):
        spark_table = 'spark_' + metadata.table_name + '_' + process_info.get('file_no')
        df.createOrReplaceTempView(spark_table)
        transformedColumns = [col for col in df.columns if col[:2] == '__']
        validateSQL = self.gen_validation_sql(transformedColumns, file_config, metadata, process_info)
        self.logger.info(validateSQL)
        data = spark.sql(validateSQL)

        #data.cache()
        errorcount = data.where('length(_error_message) > 0').count()
        if errorcount > 0:
            self._err_msg = 'Data Type not matched to KV file! Please see error file for detail.'

        return data

    def val_column_num(self, data_columns, metadata_columns, process_info):
        data_column_num = len(data_columns)
        metadata_column_num = len(metadata_columns)

        if data_column_num != metadata_column_num:
            self._err_msg = '%d columns found in data file but %d columns defined in KV file!' % (data_column_num, metadata_column_num)

    def get_error(self):
        return self._err_msg

