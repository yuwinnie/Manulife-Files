# variables
from db_credentials import hive_db_config
from hql_queries import rc_querys
from variables import *
# methods
from etl import etl_process

def main():
  print('starting etl')
	
  # establish connection for target database (sql-server)
  target_cnx = pyodbc.connect(**datawarehouse_db_config)
	
  # loop through credentials
  # firebird
  for config in fbd_db_config: 
    try:
      print("loading db: " + config['database'])
      etl_process(fbd_queries, target_cnx, config, 'firebird')
    except Exception as error:
      print("etl for {} has error".format(config['database']))
      print('error message: {}'.format(error))
      continue
	
  target_cnx.close()

if __name__ == "__main__":
  main()