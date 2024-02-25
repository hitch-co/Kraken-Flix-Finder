from flask import Flask, jsonify, request, render_template
from classes.DBExplorer import DBExplorer

from classes.ConfigManager import ConfigManager
from classes.BQUserManager import BQUserManager
from classes.LoggingManager import LoggingManager

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

        except Exception as e:
            print(f"Error in MyFlaskApp.__init__(): {e}")

    def setup_routes(self):
        @self.app.route('/')
        def home():
            return render_template('index.html')

        @self.app.route('/genres', methods=['GET'])
        def genres():

            ############################################################
            # Should probably be moved to a separate function
            dbexplorer = DBExplorer(db_path=self.db_path)
            query_path = './sql/get_genres.sql'
            query = dbexplorer.load_sql_file_from_path(path=query_path)
            results = dbexplorer.query_sql(query)
            jsonified_results = jsonify(results)

            self.logger.info(f"Number of rows: {str(len(results))}")
            self.logger.info(f"Top 5 rows: {results[:5]}")
            self.logger.debug(f"JSONified results: {jsonified_results}")
            return jsonified_results
                        
        @self.app.route('/search', methods=['POST'])
        def search():

            ############################################################
            # Should probably be moved to a separate function
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

        @self.app.route('/account', methods=['GET', 'POST'])
        def account():
            if request.method == 'POST':
                # Account creation or update logic goes here
                return jsonify({'message': 'Account created/updated successfully'})
            else:
                # Account information retrieval logic goes here
                return jsonify({'message': 'Displaying account information'})
            
        @self.app.route('/login/users', methods=['POST'])
        def check_login():
            try:
                data = request.json
                username = data.get('user_login', 'notta')
                password = data.get('user_password', 'notta')

                result = self.bq_user_manager.check_user_login(username, password)
                self.logger.info(f"Type of result: {type(result)}")
                
                if username == 'visitor':
                    return jsonify(True)
                
                self.logger.info(f"User {username} login result: {result}")
                return jsonify(result)
                            
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