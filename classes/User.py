from flask_login import UserMixin

from classes.BQUserManager import BQUserManager

class User(UserMixin):
    def __init__(self, username, email, date_created, is_admin):
        self.username = username
        self.email = email
        self.date_created = date_created
        self.is_admin = is_admin
        
    # Override the get_id method
    def get_id(self):
        return self.username
    
    @staticmethod
    def get(username):
        bq_user_manager = BQUserManager()
        record = bq_user_manager.get_user(
            username = username
            )
        if record:
            user = User(
                username=record['username'], 
                email=record['email'],
                date_created=record['date_created'], 
                is_admin=record['is_admin']
            )
            return user
        return None