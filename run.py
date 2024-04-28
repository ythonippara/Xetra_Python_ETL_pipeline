""" Runs the Xetra ETL application """
import logging
import logging.config

import yaml

def main():
    """
    The entry point to run xetra ETL job.
    """
    # Parse the YAML file
    config_path = 'C:/Data/xetra_project_v2/Xetra_Python_ETL_pipeline/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    #print(config)
    # Configure logging
    # Load YAML, choose the section, use it as dictionary
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    # Define a logger. __name__ creates a root logger using the name of the file.
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")

if __name__ == '__main__':
    main()