import os
import yaml
import dotenv

from classes.LoggingManager import LoggingManager
from schemas.bq_schemas import get_bq_schemas

runtime_logger_level = 'DEBUG'

class ConfigManager:
    _instance = None

    @classmethod
    def initialize(cls, yaml_filepath):
        if cls._instance is None:
            try:
                cls._instance = object.__new__(cls)
            except Exception as e:
                print(f"Exception in object.__new__(cls): {e}")
            
            try:
                cls._instance.create_logger()
            except Exception as e:
                print(f"Exception in create_logger(): {e}")

            try:
                cls._instance.initialize_config(yaml_filepath=yaml_filepath)
            except Exception as e:
                print(f"Exception in initialize_config(): {e}")

    @classmethod
    def get_instance(cls):
        try:
            if cls._instance is None:
                raise Exception("ConfigManager is not initialized. Call 'initialize' first.")
            return cls._instance
        except Exception as e:
            print(f"Exception in get_instance(): {e}")

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
        else:
            raise Exception("You cannot create multiple instances of ConfigManager. Use 'get_instance'.")

    def initialize_config(self, yaml_filepath):
        try:
            self.load_yaml_config(yaml_full_path=yaml_filepath)
        except Exception as e:
            self.logger.error(f"Error, exception in load_yaml_config(): {e}", exc_info=True)

        try:
            self.set_env_variables()
        except Exception as e:
            self.logger.error(f"Error, exception in set_env_variables(): {e}", exc_info=True)

        try:
            self.bq_schemas = self.load_schemas()
        except Exception as e:
            self.logger.error(f"Error, exception in load_schemas(): {e}", exc_info=True)

    def load_schemas(self):
        try:
            schemas = get_bq_schemas()
            return schemas
        except Exception as e:
            self.logger.error(f"Error in load_schemas(): {e}")
            
    def load_yaml_config(self, yaml_full_path):
        try:
            with open(yaml_full_path, 'r') as file:
                self.logger.debug("loading individual configs...")
                yaml_config = yaml.safe_load(file)
                self.update_config_from_yaml(yaml_config)
                self.set_gcp_env_credentials(yaml_config)
                            
        except FileNotFoundError:
            self.logger.error(f"YAML configuration file not found at {yaml_full_path}")
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration: {e}")
        except Exception as e:
            self.logger.error(f"Error in load_yaml_config(): {e}")

    def set_env_variables(self):
        if self.env_file_directory and self.env_file_name:
            env_path = os.path.join(self.env_file_directory, self.env_file_name)
            if os.path.exists(env_path):
                dotenv.load_dotenv(env_path)
                self.update_config_from_env()
                self.update_config_from_env_set_at_runtime()
            else:
                self.logger.error(f".env file not found at {env_path}")

    def update_config_from_env_set_at_runtime(self):
        try:
            self.logger.warning("No environment variables to update at runtime.")
        except Exception as e:
            self.logger.error(f"Error in update_config_from_env_set_at_runtime(): {e}")

    def update_config_from_env(self):
        try:
            self.logger.warning("No environment variables to update.")
        except Exception as e:
            self.logger.error(f"Error in update_config_from_env(): {e}")

    def set_gcp_env_credentials(self, yaml_config):
        try:
            self.keys_dirpath = yaml_config['keys_dirpath']
            self.google_service_account_credentials_file = yaml_config['google_service_account_credentials_file']

            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
                self.keys_dirpath, 
                self.google_service_account_credentials_file
                )
        except Exception as e:
            self.logger.error(f"Error in set_gcp_env_credentials(): {e}\nTraceback:", exc_info=True)

    def update_config_from_yaml(self, yaml_config):
        try:
            # Update instance variables with YAML configurations
            self.env_file_directory = yaml_config['env_file_directory']
            self.env_file_name = yaml_config['env_file_name']
    
            self.config_dirpath = yaml_config['config_dirpath']
            self.keys_dirpath = yaml_config['keys_dirpath']

            # BQ config items
            self.bq_project_id = yaml_config['bq_config']['project_id']
            self.bq_dataset_id = yaml_config['bq_config']['dataset_id']
            self.bq_table_id_user_login = yaml_config['bq_config']['table_ids']['user_login']

        except Exception as e:
            self.logger.error(f"Error in update_config_from_yaml(): {e}")

    def create_logger(self):
        self.logger = LoggingManager(
            dirname='log',
            logger_name='logger_ConfigManagerClass', 
            debug_level=runtime_logger_level,
            mode='w',
            stream_logs=True,
            encoding='UTF-8'
            ).create_logger()

def main(yaml_filepath):
    ConfigManager.initialize(yaml_filepath)
    config = ConfigManager.get_instance()
    return config

if __name__ == "__main__":
    yaml_filepath = r'C:\Users\Admin\OneDrive\Desktop\_work\__repos (unpublished)\_____CONFIG\crube_videos_database\config\config.yaml'
    print(f"yaml_filepath_type: {type(yaml_filepath)}")
    print(yaml_filepath)
    config = main(yaml_filepath)
    
    print(config.google_service_account_credentials_file)
    print(config.bq_dataset_id)
    print(config.bq_project_id)
    print(config.bq_table_id_user_login)