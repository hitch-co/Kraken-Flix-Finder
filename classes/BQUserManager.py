from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from google.cloud.exceptions import NotFound, GoogleCloudError
from datetime import datetime
import time

from classes.ConfigManager import ConfigManager
from classes.LoggingManager import LoggingManager

runtime_debug_level = 'DEBUG'

class BQUserManager:
    def __init__(self):
        self.logger = LoggingManager(
            dirname='log', 
            logger_name='logger_BQUploader',
            debug_level=runtime_debug_level,
            mode='w',
            stream_logs=True
            ).create_logger()
        self.config = ConfigManager.get_instance()

        self.bq_client = bigquery.Client()
        self.logger.info("BQUploader initialized.")

    def save_list_of_movie_ids(self, username, list_name, movie_ids: list[int]):
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_users_saved_lists
        )
        self.logger.debug(f"saving list '{list_name}' for user '{username}'")
        self.logger.debug(f"Inserting new movie IDs to table: {fully_qualified_table_id}")
        self.logger.debug(f"Movie IDs: {movie_ids}")

        rows_to_insert = [
            {"username": username, "list_name": list_name, "movie_id": movie_id, "is_active": True}
            for movie_id in movie_ids
        ]

        self.logger.debug(f"rows_to_insert: {rows_to_insert}")

        errors = self.bq_client.insert_rows_json(fully_qualified_table_id, rows_to_insert)
        if errors == []:
            self.logger.debug(f"New movie IDs for {username}'s list '{list_name}' have been saved successfully.")
            self.logger.debug(f"Movie IDs: {movie_ids}")
            return movie_ids
        else:
            self.logger.error(f"Errors while inserting new movie IDs for {username}'s list '{list_name}': {errors}")
            return None
        
    def _replace_bq_table(self, fully_qualified_table_id, schema_name):
        """
        Replaces a BigQuery table with the given schema. Deletes the existing table if it exists, then creates a new one.
        """
        try:
            schema = self.config.bq_schemas[schema_name]
            table_ref = bigquery.TableReference.from_string(fully_qualified_table_id)
            self.logger.info(f"Attempting to replace BQ table: {fully_qualified_table_id}")

            # Attempt to delete the table (if it exists)
            self.bq_client.delete_table(table_ref, not_found_ok=True)  # not_found_ok=True makes this operation idempotent
            self.logger.info(f"Table {fully_qualified_table_id} deleted (if it existed).")

            # Create a new table with the provided schema
            table = bigquery.Table(table_ref, schema=schema)
            self.bq_client.create_table(table)
            self.logger.info(f"New table {fully_qualified_table_id} created successfully.")
        except Exception as e:
            self.logger.error(f"An error occurred during table replacement: {e}")
            return None
    
    def _create_table_if_not_exists(
        self,
        fully_qualified_table_id,
        schema_name
        ):
        """
        Creates or replaces a BigQuery table with the given schema in the target dataset.
        """
        schema = self.config.bq_schemas[schema_name]
        try:
            table_ref = bigquery.TableReference.from_string(fully_qualified_table_id)
            self.logger.info(f"Target BQ table: {fully_qualified_table_id}")

            try:
                self.bq_client.get_table(table_ref)
                self.logger.info(f"{fully_qualified_table_id} already exists so a new one was not created. continuing with job load.")
            except NotFound:
                self.logger.warning(f"{fully_qualified_table_id} was not found. Creating new table.")
                table = bigquery.Table(table_ref, schema=schema)
                self.bq_client.create_table(table)
                self.logger.info(f"{fully_qualified_table_id} created, continuing with job load.")

        except GoogleCloudError as e:
            self.logger.error(f"Google Cloud Error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return None

    def _generate_fully_qualified_table_id(self, project_id, dataset_id, table_id) -> str:
        return f"{project_id}.{dataset_id}.{table_id}"

    def _send_queryjob_to_bq(self, query) -> None:
        try:
            self.logger.info("Starting BigQuery _send_queryjob_to_bq() job...")
            query_job = self.bq_client.query(query)

            self.logger.debug(f"Executing query...")
            query_job.result()

            self.logger.info(f"Query job {query_job.job_id} completed successfully.")

        except GoogleAPIError as e:
            self.logger.error(f"BigQuery job failed: {e}")

        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        else:
            # Optionally, get and log job statistics
            job_stats = query_job.query_plan
            self.logger.debug(f"Query plan: {job_stats}")

    def _send_recordsjob_to_bq(self, full_table_id, records:list[dict]) -> None:
        self.logger.info("Starting BigQuery _send_recordsjob_to_bq() job...")
        table = self.bq_client.get_table(full_table_id)
        errors = self.bq_client.insert_rows_json(table, records)     
        if errors:
            self.logger.error(f"Encountered errors while inserting rows: {errors}")
            self.logger.error("These are the records:")
            self.logger.error(records)
        else:
            self.logger.info(f"{len(records)} successfully inserted into table_id: {full_table_id}")
            self.logger.debug("These are the records:")
            self.logger.debug(records)

    # Not in use yet
    def execute_new_user_creation(self, new_user_record):
        # Generate fully qualified table ID
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_user_login
        )

        # Create or replace a BQ table
        self._create_table_if_not_exists(
            fully_qualified_table_id=fully_qualified_table_id,
            schema_name='user_login_schema'
        )

        # Generate a merge query for the new user record
        query_for_user_insert = self._generate_insert_query_if_not_exists(
            fully_qualified_table_id=fully_qualified_table_id,
            record=new_user_record,
            schema_name='user_login_schema'
        )

        # Send the query to BQ
        self._send_queryjob_to_bq(query_for_user_insert)

    def check_user_login(self, username, password) -> tuple[bool, list[dict]]:
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_user_login
            )
        
        query = f"""
        SELECT DISTINCT
            username, email, date_created, is_active, is_admin 
        FROM {fully_qualified_table_id}
        WHERE username = '{username}' AND password = '{password}'
        """

        # Execute the query
        query_job = self.bq_client.query(query)

        # Process the results
        results = [row for row in query_job]
        if results:
            return True, results[0]
        else:
            return False, None

    def get_user(self, username):
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_user_login
        )
        query = f"""
        SELECT DISTINCT * FROM {fully_qualified_table_id}
        WHERE username = '{username}'
        """

        # Execute the query
        query_job = self.bq_client.query(query)

        # Process the results
        results = [row for row in query_job]
        if results:
            return results[0]
        else:
            return None

    def get_saved_list_movie_ids(self, username, list_name, is_active='TRUE'):
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_users_saved_lists
        )

        # Convert is_active to boolean
        is_active_bool = True if is_active.upper() == 'TRUE' else False

        # Prepare the parameterized query
        query = f"""
        SELECT DISTINCT movie_id
        FROM `{fully_qualified_table_id}`
        WHERE username = @username AND list_name = @list_name AND is_active = @is_active
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("username", "STRING", username),
                bigquery.ScalarQueryParameter("list_name", "STRING", list_name),
                bigquery.ScalarQueryParameter("is_active", "BOOL", is_active_bool),
            ]
        )

        try:
            # Execute the query
            query_job = self.bq_client.query(query, job_config=job_config)

            # Wait for the query to finish
            results = query_job.result()

            # Process the results
            movie_ids = [row.movie_id for row in results]

            self.logger.debug(f"Movie IDs for {username}'s list '{list_name}' with is_active={is_active}: {movie_ids}")
            return movie_ids
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
    
    def get_saved_list_names(self, username):
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_users_saved_lists
        )

        query = f"""
        SELECT DISTINCT list_name
        FROM `{fully_qualified_table_id}`
        WHERE username = @session_username
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("session_username", "STRING", username),
            ]
        )

        query_job = self.bq_client.query(query, job_config=job_config)
        
        # Convert each row to a dict
        results = [dict(row) for row in query_job]
        return results

    def add_list_to_saved_lists(self, username, list_name, movie_ids:list[str]) -> None:
        fully_qualified_table_id = self._generate_fully_qualified_table_id(
            self.config.bq_project_id,
            self.config.bq_dataset_id,
            self.config.bq_table_id_users_saved_lists
        )

        records = []
        for movie_id in movie_ids:
            record = {
                "username": username,
                "list_name": list_name,
                "movie_id": movie_id,
                "is_active": True
            }
            records.append(record)

        self._send_recordsjob_to_bq(fully_qualified_table_id, records)
   
    def ____create_or_replace_bq_table_from_gcs(
            self,
            project_name, 
            source_bucket_name,
            source_dir_path,
            source_file_name,
            target_dataset_name, 
            target_table_name,
            schema
            ):
        
        self.logger.debug('---------------------------------')
        self.logger.debug(f"project_name: {project_name}")
        self.logger.debug(f"source_bucket_name: {source_bucket_name}")
        self.logger.debug(f"source_dir_path: {source_dir_path}")
        self.logger.debug(f"source_file_name: {source_file_name}")
        self.logger.debug(f"target_dataset_name: {target_dataset_name}")
        self.logger.debug(f"target_table_name: {target_table_name}")
        self.logger.debug(f"schema: {schema}")
        self.logger.debug(f"")

        try:
            gcs_uri = f"gs://{source_bucket_name}/{source_dir_path}/{source_file_name}"
            table_fullqual = f"{project_name}.{target_dataset_name}.{target_table_name}"
            table_ref = bigquery.TableReference.from_string(table_fullqual)

            self.logger.info(f"Source URI from GCS is: {gcs_uri}")
            self.logger.info(f"Target BQ table: {table_fullqual}")

            try: # Check if the table already exists in BQ
                self.bq_client.get_table(table_ref)
                self.logger.info(f"{table_fullqual} already exists so a new one was not created. continuing with job load for {gcs_uri}.")
            except NotFound:
                self.logger.warning(f"{table_fullqual} was not found. Creating new table.")
                table = bigquery.Table(table_ref, schema=schema)
                self.bq_client.create_table(table)
                self.logger.info(f"{table_fullqual} created, continuing with job load.")

            # Configure the external data source and start the BigQuery Load job
            job_config = bigquery.LoadJobConfig(
                autodetect=False,
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE   
            )
            load_job = self.bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job_result = load_job.result()

            self.logger.info(f"This is the load_job_result: {load_job_result}")
            return load_job_result

        except GoogleCloudError as e:
            self.logger.error(f"Google Cloud Error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return None

    def _generate_insert_query_if_not_exists(
            self, 
            fully_qualified_table_id, 
            record: dict, 
            schema_name: str,
            unique_field_name: str = 'username',
            date_fields: list[str] = ['date_created']
            ) -> str:
        # Access the schema 
        schema = self.config.bq_schemas.get(schema_name, [])

        # Construct field list for INSERT and SELECT
        fields = [field.name for field in schema]

        # Construct values list for INSERT and SELECT
        values = []
        for field in fields:
            if field in date_fields:
                value = f"CAST('{record[field]}' AS DATETIME)"
            elif isinstance(record[field], str):
                value = f"'{record[field]}'"
            else:
                value = str(record[field])
            values.append(value)

        # Construct the INSERT statement dynamically
        insert_query = f"""
            INSERT INTO {fully_qualified_table_id} ({', '.join(fields)})
            SELECT * FROM (SELECT {', '.join(values)}) AS new_record
            WHERE NOT EXISTS (
                SELECT 1 FROM {fully_qualified_table_id} WHERE {unique_field_name} = '{record[unique_field_name]}'
            );
        """

        self.logger.debug("The insert query for a new user was generated")
        self.logger.debug(insert_query)
        return insert_query
 
    def ____generate_merge_query_from_listdict(self, table_id, records: list[dict]) -> str:

        # Build the UNION ALL part of the query
        union_all_query = " UNION ALL ".join([
            f"SELECT '{viewer['user_id']}' as user_id, '{viewer['user_login']}' as user_login, "
            f"'{viewer['user_name']}' as user_name, PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '{viewer['timestamp']}') as last_seen"
            for viewer in records
        ])
        
        # Add the union all query to our final query to be sent to BQ jobs
        merge_query = f"""
            MERGE {table_id} AS target
            USING (
                {union_all_query}
            ) AS source
            ON target.user_id = source.user_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.user_login = source.user_login,
                    target.user_name = source.user_name,
                    target.last_seen = source.last_seen
            WHEN NOT MATCHED THEN
                INSERT (user_id, user_login, user_name, last_seen)
                VALUES(source.user_id, source.user_login, source.user_name, source.last_seen);
        """
        
        self.logger.debug("The users table query was generated")
        self.logger.debug("This is the users table merge query:")
        self.logger.debug(merge_query)
        return merge_query

    def ____fetch_results_as_dict(self, table_id) -> list[dict]:
        query = f"""
        SELECT DISTINCT user_id, user_login, user_name
        FROM `{table_id}`
        """

        # Execute the query
        query_job = self.bq_client.query(query)

        # Process the results
        results = []
        for row in query_job:
            results.append({
                "user_id": row.user_id,
                "user_login": row.user_login,
                "user_name": row.user_name
            })

        return results
    
    def ____fetch_results_as_text(self, table_id) -> str:
        # Construct a query to count occurrences of specific commands in a case-insensitive manner
        query = f"""
        SELECT
            SUM(CASE WHEN LOWER(content) LIKE '!chat%' THEN 1 ELSE 0 END) as chat_count,
            SUM(CASE WHEN LOWER(content) LIKE '!startstory%' THEN 1 ELSE 0 END) as startstory_count,
            SUM(CASE WHEN LOWER(content) LIKE '!addtostory%' THEN 1 ELSE 0 END) as addtostory_count,
            SUM(CASE WHEN LOWER(content) LIKE '!what%' THEN 1 ELSE 0 END) as what_count,
            SUM(CASE WHEN LOWER(content) LIKE '@chatzilla_ai%' THEN 1 ELSE 0 END) as chatzilla_shoutouts            
        FROM `{table_id}`
        """

        # Execute the query and fetch the result
        result = self.bq_client.query(query).result()
        self.logger.debug(f"Result (type: {type(result)}): {result}")

        # Convert RowIterator to a list and get the first row
        result_list = list(result)[0]  # 'result' is the RowIterator from BQ query
        if result_list:
            stats_text = f"""
            Historic !commands usage:\n
            \n!chat: {result_list.chat_count}
            !startstory: {result_list.startstory_count}
            !addtostory: {result_list.addtostory_count}
            !what: {result_list.what_count}
            """

        # Log the formatted stats
        self.logger.debug(f"Formatted Stats: {stats_text}")

        return stats_text

    def ____fetch_results_as_list(self, table_id) -> list[str]:
        query = f"""
        SELECT DISTINCT user_name FROM `{table_id}`
        """

        # Execute the query
        query_job = self.bq_client.query(query)

        # Process the results
        results = [row.user_name for row in query_job]

        return results

    def ____generate_records_from_listdict(self, records: list[dict]) -> list[dict]:
        rows_to_insert = []
        for record in records:
            user_id = record.get('user_id')
            channel = record.get('channel')
            content = record.get('content')
            timestamp = record.get('timestamp')
            user_badges = record.get('badges')
            color = record.get('tags').get('color', '') if record.get('tags') else ''
            
            row = {
                "user_id": user_id,
                "channel": channel,
                "content": content,
                "timestamp": timestamp,
                "user_badges": user_badges,
                "color": color                
            }
            rows_to_insert.append(row)

        self.logger.debug("These are the user interactions records (rows_to_insert):")
        self.logger.debug(rows_to_insert[0:2])
        return rows_to_insert   

if __name__ == '__main__':
    ConfigManager.initialize(r'C:\Users\Admin\OneDrive\Desktop\_work\__repos (unpublished)\_____CONFIG\crube_videos_database\config\config.yaml')
    config = ConfigManager.get_instance()
    bq_user_manager = BQUserManager()


    # ################################################################
    # # TEST 1: Generate user
    # # Generate user records for the user_login table based ont his schema:
    # new_user_record ={
    #     "username": "cred",
    #     "email": "unknown",
    #     "password": "wer",
    #     "date_created": "2024-02-27 00:00:00",
    #     "is_active": True,
    #     "is_admin": False
    #     }
    # # Execute the new user creation
    # bq_user_manager.execute_new_user_creation(new_user_record)


    # ################################################################
    # # TEST 2: Check user login
    # # Check user login
    # username = "cred"
    # password = "wer"
    # result, records = bq_user_manager.check_user_login(username, password)
    # print(f"User {username} login result: {result}")
    # print(f"Type of result: {type(result)}")
    # print(f"Records: {records}")


    # #################################################################
    # # # TEST 3: Create Saved List Table
    # # # Create or replace a BQ table
    # fully_qualified_table_id = bq_user_manager._generate_fully_qualified_table_id(
    #     project_id=config.bq_project_id,
    #     dataset_id=config.bq_dataset_id,
    #     table_id=config.bq_table_id_users_saved_lists
    # )
    # bq_user_manager._replace_bq_table(
    #     fully_qualified_table_id=fully_qualified_table_id,
    #     schema_name='users_saved_lists_schema'
    # )


    # #################################################################
    # # # TEST 4: Add list to saved lists
    # username = "ehitch"
    
    # list_name = "test_favourites2"
    # movie_ids = [1587, 60]
    # bq_user_manager.add_list_to_saved_lists(username, list_name, movie_ids)
    
    # list_name = "test_favourites3"
    # movie_ids = [66, 1398]
    # bq_user_manager.add_list_to_saved_lists(username, list_name, movie_ids)
    
    # list_name = "test_favourites4"
    # movie_ids = [82, 84]
    # bq_user_manager.add_list_to_saved_lists(username, list_name, movie_ids)
    
    # list_name = "test_favourites5"
    # movie_ids = [1614, 91]
    # bq_user_manager.add_list_to_saved_lists(username, list_name, movie_ids)