import json


class Config(object):
    def __init__(self, sc, configfile):
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self.configdict = {}

        if configfile.rfind('/') >= 0:
            common_config_path = configfile[:configfile.rfind('/') + 1] + 'config_common.json'
        else:
            common_config_path = 'config_common.json'

        self.logger.info('loading common config file %s' % common_config_path)

        with open(common_config_path) as f:
            commonconfigdict = json.load(f)
            self.configdict.update(commonconfigdict)

        self.logger.info('common config file %s is successfully loaded' % common_config_path)

        self.logger.info('loading config file %s' % configfile)

        with open(configfile) as f:
            feedconfigdict = json.load(f)
            self.configdict.update(feedconfigdict)

        self.logger.info('config file %s is successfully loaded' % configfile)

    def get(self, key):
        return self.configdict.get(key, '')


if __name__ == "__main__":
    import logging.config
    import os

    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

    configfile = '../conf/config_ta_asia_hk.json'
    config = Config(configfile)

    print(config.get('typedDB'))
