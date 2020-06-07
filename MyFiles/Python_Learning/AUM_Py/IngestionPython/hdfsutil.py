from py4j.java_gateway import java_import
import os


class HDFSUtil(object):
    def __init__(self, sc):
        self._sc = sc
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self.jvm = sc._gateway.new_jvm_view()
        java_import(self.jvm, "java.net.URI")
        java_import(self.jvm, "org.apache.hadoop.fs.Path")
        java_import(self.jvm, "org.apache.hadoop.fs.FileSystem")
        self.hdfs = self.jvm.FileSystem.get(sc._jsc.hadoopConfiguration())

    def delete_temp_folders(self, datafolders):
        for datafolder in datafolders:
            if self.hdfs.exists(self.jvm.Path(datafolder + '/_temporary')):
                self.logger.info('deleting _temporary in %s' % datafolder)
                self.hdfs.delete(self.jvm.Path(datafolder + '/_temporary'), True)
                self.logger.info('deleted _temporary in %s' % datafolder)

    def listHDFSTmpFiles(self, config):
        hdfsfiles = []
        hdfs_tmp_base_folder = config.get('hdfs_tmp_base_folder')
        source = config.get('source')
        sub_source = config.get('sub_source')

        if sub_source:
            hdfs_tmp_folder = hdfs_tmp_base_folder + '/' + source + '/' + sub_source
        else:
            hdfs_tmp_folder = hdfs_tmp_base_folder + '/' + source

        file_status_list = self.hdfs.listStatus(self.jvm.Path(hdfs_tmp_folder))

        for file_status in file_status_list:
            if file_status.isFile():
                filepath = file_status.getPath().toString()
                hdfsfiles.append(filepath)
                self.logger.info('file %s is found' % filepath)

        return hdfsfiles

    def getFullPathList(self, config, fileliststr):
        filelist = fileliststr.split(',')

        hdfs_tmp_base_folder = config.get('hdfs_tmp_base_folder')
        source = config.get('source')
        sub_source = config.get('sub_source')

        if sub_source:
            hdfs_tmp_folder = hdfs_tmp_base_folder + '/' + source + '/' + sub_source
        else:
            hdfs_tmp_folder = hdfs_tmp_base_folder + '/' + source

        hdfsfiles = [hdfs_tmp_folder + '/' + file for file in filelist]

        return hdfsfiles

    def checkDuplicate(self, config, fileconfig, hdfsfile):
        self.logger.debug("checking duplicates for %s" % hdfsfile)
        hdfs_archive_base_folder = config.get('hdfs_archive_base_folder')
        source = config.get('source')
        sub_source = config.get('sub_source')
        topic = fileconfig.get('topic')

        if sub_source:
            hdfs_archive_folder = hdfs_archive_base_folder + '/' + source + '/' + sub_source + '/' + topic
        else:
            hdfs_archive_folder = hdfs_archive_base_folder + '/' + source + '/' + topic

        filename = os.path.basename(hdfsfile)
        archived_file = hdfs_archive_folder + '/' + filename

        self.logger.debug("checking if archived file %s exists" % archived_file)
        if self.hdfs.exists(self.jvm.Path(archived_file)):
            self.logger.debug("%s is found to be a duplicate" % hdfsfile)
            return True
        else:
            self.logger.debug("%s is not found to be a duplicate" % hdfsfile)
            return False

    def move_to_archive(self, config, fileconfig, hdfsfile):
        hdfs_archive_base_folder = config.get('hdfs_archive_base_folder')
        source = config.get('source')
        sub_source = config.get('sub_source')
        topic = fileconfig.get('topic')

        if sub_source:
            hdfs_archive_folder = hdfs_archive_base_folder + '/' + source + '/' + sub_source + '/' + topic
        else:
            hdfs_archive_folder = hdfs_archive_base_folder + '/' + source + '/' + topic

        filename = os.path.basename(hdfsfile)
        archivefile = hdfs_archive_folder + '/' + filename

        if not self.hdfs.exists(self.jvm.Path(hdfs_archive_folder)):
            self.logger.debug('archive folder %s does not exists' % hdfs_archive_folder)
            if self.hdfs.mkdirs(self.jvm.Path(hdfs_archive_folder)):
                self.logger.debug('archive folder %s was created' % hdfs_archive_folder)
            else:
                self.logger.error('archive folder %s was not created' % hdfs_archive_folder)
                raise Exception('archive folder %s was not created' % hdfs_archive_folder)

        if self.hdfs.rename(self.jvm.Path(hdfsfile), self.jvm.Path(archivefile)):
            self.logger.info("%s moved to archive folder %s." % (hdfsfile, hdfs_archive_folder))
        else:
            self.logger.info("failed to move %s to archive folder %s." % (hdfsfile, hdfs_archive_folder))
            raise Exception("failed to move %s to archive folder %s." % (hdfsfile, hdfs_archive_folder))

    def move_to_error_archive(self, config, fileconfig, hdfsfile, process_info):
        hdfs_error_archive_base_folder = config.get('hdfs_error_archive_base_folder')
        source = config.get('source')
        sub_source = config.get('sub_source')
        topic = fileconfig.get('topic')

        if sub_source:
            hdfs_error_archive_folder = hdfs_error_archive_base_folder + '/' + source + '/' + sub_source + '/' + topic
        else:
            hdfs_error_archive_folder = hdfs_error_archive_base_folder + '/' + source + '/' + topic

        filename = os.path.basename(hdfsfile)
        errorarchivefile = hdfs_error_archive_folder + '/' + filename + '.' + self._sc.applicationId

        if not self.hdfs.exists(self.jvm.Path(hdfs_error_archive_folder)):
            self.logger.debug('archive folder %s does not exists' % hdfs_error_archive_folder)
            if self.hdfs.mkdirs(self.jvm.Path(hdfs_error_archive_folder)):
                self.logger.debug('archive folder %s was created' % hdfs_error_archive_folder)
            else:
                self.logger.error('archive folder %s was not created' % hdfs_error_archive_folder)
                raise Exception('archive folder %s was not created' % hdfs_error_archive_folder)

        if self.hdfs.rename(self.jvm.Path(hdfsfile), self.jvm.Path(errorarchivefile)):
            self.logger.info("%s moved to archive folder %s." % (hdfsfile, hdfs_error_archive_folder))
            process_info['error_archive_path'] = errorarchivefile
        else:
            self.logger.info("failed to move %s to archive folder %s." % (hdfsfile, hdfs_error_archive_folder))
            raise Exception("failed to move %s to archive folder %s." % (hdfsfile, hdfs_error_archive_folder))

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import config

    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("ingestion") \
        .master("local[2]") \
        .getOrCreate()

    sc = spark.sparkContext

    config = config.Config(sc, "../conf/config_ta_asia_test.json")

    file_config = config.get('files')[3]
    hdfsutil = HDFSUtil(sc)



