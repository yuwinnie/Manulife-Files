from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import current_timestamp, to_timestamp, unix_timestamp, lit
import os, sys
import argparse
from datetime import datetime
import getpass
from threading import Thread
try:
    from Queue import Queue
except ImportError:
    from queue import Queue
from config import Config
from hdfsutil import HDFSUtil
from reader import Reader
from metadata import Metadata
from transform import Transformer
from validate import Validator
from writer import Writer
from emailsender import EmailSender


def getFileConfig(hdfsfile, config):
    import fnmatch

    filename = os.path.basename(hdfsfile)

    for fileconfig in config.get('files'):
        file_pattern = fileconfig.get('pattern')

        if fnmatch.fnmatch(filename, file_pattern):
            logger.info("file %s matched to pattern %s" % (filename, file_pattern))
            file_config = fileconfig
            return file_config


def ingest(hdfsfile, file_no, datafolders):
    """

    :type hdfsfiles: str
    :type config: config.Config
    """
    try:
        process_info = dict()
        process_info['process_start_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        process_info['user_name'] = getpass.getuser()
        process_info['file_name'] = os.path.basename(hdfsfile)
        process_info['file_no'] = file_no
        #spark = HiveContext(spark.sparkContext)
        file_config = getFileConfig(hdfsfile, config)

        if not file_config:
            err_msg = "can not find matched file configuration for file %s!" % hdfsfile
            logger.error(err_msg)
            return 'Error - ' + err_msg

        metadata = Metadata(sc)

        if hdfsutil.checkDuplicate(config, file_config, hdfsfile):
            hdfsutil.move_to_error_archive(config, file_config, hdfsfile, process_info)
            metadata.log_error_table(spark, file_config, config, process_info, "duplicate file")
            return 'Error - a file with same name already exists in archive folder!' + \
                   ' Moved to error_archive "' + process_info['error_archive_path'] + '".'

        metadata.loadKVFile(spark, config, file_config.get('kv_file'), process_info)

        reader = Reader(spark)
        df = reader.read(hdfsfile, file_config, metadata, process_info)

        process_info['row_count'] = df.count()

        logger.info("row count in file %s is %d" % (hdfsfile, process_info['row_count']))

        if process_info['row_count'] == 0:
            if file_config.get('empty_check', "no").lower() == "yes":
                hdfsutil.move_to_error_archive(config, file_config, hdfsfile, process_info)
                metadata.log_error_table(spark, file_config, config, process_info, "file is empty")
                return 'Error - Empty File!' + \
                    ' Moved to error_archive "' + process_info['error_archive_path'] + '".'
            else:
                hdfsutil.move_to_archive(config, file_config, hdfsfile)
                logger.warn('%s is empty!' % hdfsfile)
                return 'Success'

        validator = Validator(sc)
        validator.val_column_num(df.columns, metadata.data_types, process_info)
        val_error = validator.get_error()
        if val_error:
            hdfsutil.move_to_error_archive(config, file_config, hdfsfile, process_info)
            metadata.log_error_table(spark, file_config, config, process_info, val_error)
            return 'Error - ' + val_error + \
                ' Moved to error_archive "' + process_info['error_archive_path'] + '".'

        transformer = Transformer(sc)
        df = transformer.trans_data_types(spark, df, file_config, metadata, process_info)
        df = validator.val_data_types(spark, df, file_config, metadata, process_info)

        transformedColumns = [col for col in df.columns if col[:2] == '__']

        writer = Writer(sc)
        val_error = validator.get_error()
        if val_error:
            error_df = df.where('length(_error_message) > 0').drop(*transformedColumns)
            writer.write_errorfile(error_df, config, file_config, process_info)

            df.unpersist()
            logger.error('file %s failed at data type validation' % hdfsfile)
            hdfsutil.move_to_error_archive(config, file_config, hdfsfile, process_info)
            metadata.log_error_table(spark, file_config, config, process_info, val_error)
            return 'Error - ' + val_error + \
                ' Moved to error_archive "' + process_info['error_archive_path'] + '".' + \
                ' Error file path "' + process_info['error_file_path'] + '".'
        else:
            orig_columns = [col[1:] for col in transformedColumns]
            data_df = df.drop(*orig_columns) \
                .drop('_error_message') \
                .withColumn('source_filename', lit(os.path.basename(hdfsfile))) \
                .withColumn('process_timestamp', to_timestamp(lit(process_info['process_start_timestamp']), 'yyyy-MM-dd HH:mm:ss'))

            writer.write_orc(data_df, spark, config, metadata, process_info)
            datafolders.add(process_info['hdfs_datafile_path'])

            df.unpersist()
            hdfsutil.move_to_archive(config, file_config, hdfsfile)
            process_info['process_end_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            metadata.log_registry_table(spark, file_config, config, process_info)
            logger.info('file %s has been successfully ingested' % hdfsfile)
            return 'Success'
    except Exception as e:
        logger.error('file %s ingestion failed! exception is %s' % (hdfsfile, str(e)))
        return 'Failed - Exception happened! Please see Yarn log for details'


def parallel_ingest(q, results, datafolders):
    while not q.empty():
        work = q.get()  # fetch new work from the Queue
        spark_pool = 'pool' + str(work[0])
        sc.setLocalProperty("spark.scheduler.pool", spark_pool)
        ing_status = ingest(work[1], str(work[0]), datafolders)
        logger.info("Completed..." + work[1])
        results[work[1]] = ing_status
        # signal to the queue that task has been processed
        sc.setLocalProperty("spark.scheduler.pool", None)
        q.task_done()
    return True


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("ingestion") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.orc.impl", "hive") \
        .getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("IDDLLogger")

    logger.info("Ingestion started -- %s" % datetime.now())

    parser = argparse.ArgumentParser()
    parser.add_argument("configfile", help="configuration file")
    parser.add_argument("filelist", help="list of data files")
    args = parser.parse_args()

    config = Config(sc, args.configfile)
    fileliststr = args.filelist

    emailsender = EmailSender(sc, config)
    hdfsutil = HDFSUtil(sc)

    try:
        if fileliststr:
            hdfsfiles = hdfsutil.getFullPathList(config, fileliststr)
        else:
            hdfsfiles = hdfsutil.listHDFSTmpFiles(config)

        logger.info("file_count = " + str(len(hdfsfiles)))
        logger.info(hdfsfiles)

        if len(hdfsfiles) == 0:
            logger.error("no file was found in hdfs tmp folder!")
            emailsender.send_error_email("no file was found in hdfs tmp folder!")
            # spark.stop()
            exit(1)
    except Exception as e:
        logger.error("Failed to list files in hdfs tmp folder. Exception: %s" % str(e))
        emailsender.send_error_email("Failed to list files in hdfs tmp folder. Exception: %s" % str(e))
        # spark.stop()
        exit(2)

    queue = Queue(maxsize=0)
    num_threads = min(8, len(hdfsfiles))

    status_results = {filename: 'Unprocessed' for filename in hdfsfiles}
    datafolders = set()

    for i in range(len(hdfsfiles)):
        queue.put((i, hdfsfiles[i]))

    for i in range(num_threads):
        logger.info('starting worker %d' % i)
        worker = Thread(target=parallel_ingest, args=(queue, status_results, datafolders))
        worker.setDaemon(True)
        worker.start()

    queue.join()
    logger.info('All tasks completed.')

    logger.info('deleting _temporary folders')
    try:
        hdfsutil.delete_temp_folders(datafolders)
    except Exception as e:
        emailsender.send_error_email("Failed to delete _temporary folder. Exception: %s" % str(e))

    if sys.version_info < (3,):
        statusvalues = status_results.values()
    else:
        statusvalues = list(status_results.values())

    if statusvalues.count('Success') != len(statusvalues):
        logger.info("sending email")
        emailsender.send_detail_error_email(status_results)

    spark.stop()

    # if statusvalues.count('Success') != len(statusvalues):
    #     sys.exit(1)
    # else:
    #     sys.exit(0)
