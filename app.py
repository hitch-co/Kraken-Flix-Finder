from flask import Flask
from flask import session, jsonify, request, render_template
from flask_login import LoginManager, login_user, current_user
from datetime import timedelta

from classes.DBExplorer import DBExplorer
from classes.ConfigManager import ConfigManager
from classes.BQUserManager import BQUserManager
from classes.LoggingManager import LoggingManager
from classes.User import User

class MyFlaskApp:
    def __init__(
            self, 
            db_path='./db/MyVideos116/MyVideos116.db',
            yaml_filepath=r'.\config\config.yaml'
            ):
        try:
            # ConfigManager instance
            ConfigManager.initialize(yaml_filepath=yaml_filepath)
            self.config = ConfigManager.get_instance()

            # BQUserManager instance
            self.bq_user_manager = BQUserManager()

            # LoggingManager instance
            self.logger = LoggingManager(
                dirname='log', 
                logger_name='app', 
                debug_level='DEBUG', 
                mode='w', 
                stream_logs=True, 
                encoding='UTF-8'
                ).create_logger()

            # Set instance variables
            self.db_path = db_path

            # Flask app instance
            self.app = Flask(__name__)
            self.setup_routes()
            self.app.config['SECRET_KEY'] = self.config.flask_login_secret_key
            self.login_manager = LoginManager()
            self.login_manager.init_app(self.app)
            self.app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

            @self.login_manager.user_loader
            def load_user(username):
                return User.get(username=username)


        except Exception as e:
            print(f"Error in MyFlaskApp.__init__(): {e}")

    def setup_routes(self):
        @self.app.route('/')
        def home():
            return render_template('index.html')

        @self.app.route('/genres', methods=['GET'])
        def genres():
            dbexplorer = DBExplorer(db_path=self.db_path)
            query_path = './sql/get_genres.sql'
            query = dbexplorer.load_sql_file_from_path(path=query_path)
            results = dbexplorer.query_sql(query)
            jsonified_results = jsonify(results)

            self.logger.info(f"Number of rows: {str(len(results))}")
            self.logger.info(f"Top 5 rows: {results[:5]}")
            self.logger.debug(f"JSONified results: {jsonified_results}")
            return jsonified_results

        @self.app.route('/get_saved_list_items', methods=['POST'])
        def get_saved_list_items():
            try:
                data = request.json
                username = current_user.get_id()
                list_name = data.get('list_name')
                bq_movie_ids = self.bq_user_manager.get_saved_lists_movie_ids(
                    username=username, 
                    list_name=list_name
                    )
                self.logger.debug(f"bq_movie_ids: {bq_movie_ids}")

                # Assuming movie_ids is a list of integers
                str_movie_ids = ', '.join(['?'] * len(bq_movie_ids))  # Create a string of placeholders
                self.logger.debug(f"str_movie_ids: {str_movie_ids}")

                dbexplorer = DBExplorer(db_path=self.db_path)
                query_path = './sql/get_user_saved_list_items.sql'
                query_template = dbexplorer.load_sql_file_from_path(path=query_path)
                query = query_template.format(str_movie_ids=str_movie_ids)  # Insert placeholders into query

                results = dbexplorer.query_sql(query, bq_movie_ids)  # Pass list of IDs as parameter
                jsonified_results = jsonify(results)

                self.logger.info(f"Number of rows: {str(len(results))}")
                self.logger.info(f"Top 5 rows: {results[:5]}")
                self.logger.debug(f"JSONified results: {jsonified_results}")
                return jsonified_results
            except Exception as e:
                self.logger.error(f"Error in get_saved_list_items(): {e}")
                return jsonify({'error': 'An unexpected error occurred'}), 500

        @self.app.route('/get_saved_list_names', methods=['GET'])
        def get_saved_list_names():
            username = current_user.get_id()
            results = self.bq_user_manager.get_saved_list_names(username=username)
            jsonified_results = jsonify(results)

            self.logger.info(f"Number of rows: {str(len(results))}")
            self.logger.info(f"Top 5 rows: {results[:5]}")
            self.logger.debug(f"JSONified results: {jsonified_results}")
            return jsonified_results
                        
        @self.app.route('/search', methods=['POST'])
        def search():
            dbexplorer = DBExplorer(db_path=self.db_path)
            query_path = './sql/front-end-search-query.sql'
            query = dbexplorer.load_sql_file_from_path(path=query_path)

            data = request.json
            params = {
                'uinp_movie_name': '%' + data.get('movie_name') + '%',
                'uinp_genre_id': data.get('genre_id') or None,
                'uinp_year_min': data.get('year_min', 0) or None,
                'uinp_year_max': data.get('year_max', 3000) or None
            }

            # Use the DBExplorer class to query the database
            results = dbexplorer.query_sql(query, params)

            self.logger.info(f"Number of rows: {str(len(results))}")
            self.logger.info(f"Top 5 rows: {results[:5]}")
            
            # Assuming 'results' is a list of dicts or a similar structure that jsonify can handle
            jsonified_results = jsonify(results)

            self.logger.debug(f"JSONified results: {jsonified_results}")
            return jsonified_results
    
        @self.app.route('/login/users', methods=['POST'])
        def login():
            try:
                data = request.json
                username = data.get('user_login', 'notta')
                password = data.get('user_password', 'notta')
                remember = request.form.get('remember', False) # not in use yet

                result, record = self.bq_user_manager.check_user_login(username, password)
                self.logger.info(f"check_user_login result: {result}")
                self.logger.info(f"check_user_login record: {record}")
                
                # Define the field names here, inside the login function, before using them
                field_names = ['username', 'email', 'date_created', 'is_active', 'is_admin']

                # Convert 'record' Row object to a dictionary
                if record:
                    record_dict = {field: record[i] for i, field in enumerate(field_names)}
                else:
                    record_dict = {}

                json_object = {'result': result, 'record': record_dict}
                self.logger.info(f"Type of record: {type(record_dict)}")
                self.logger.info(f"Content of record: {record_dict}")
                self.logger.info(f"Type of result: {type(result)}")
                self.logger.info(f"Content of result: {result}")

                if json_object['result'] is True:
                    user = User.get(username)
                    login_user(user, remember=remember)
                    session.permanent = True
                    self.logger.info(f"User {username} login result: {json_object['result']}")
                    return jsonify(json_object)
                
                self.logger.info(f"User {username} login result: {json_object['result']}")
                return jsonify(json_object)
                            
            except Exception as e:
                self.logger.error(f"Error in get_users(): {e}")
                self.logger.error(f"User {username} login result: {result}")
                return jsonify(False)

    def run(self):
        self.app.run(debug=False, port=3200)

if __name__ == '__main__':
    my_flask_app = MyFlaskApp(
        db_path='./db/MyVideos116/MyVideos116.db',
        yaml_filepath=r'C:\Users\Admin\OneDrive\Desktop\_work\__repos (unpublished)\_____CONFIG\crube_videos_database\config\config.yaml'
    )
    my_flask_app.run()