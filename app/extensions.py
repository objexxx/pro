from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_mail import Mail

# Initialize Extensions
db = SQLAlchemy()
login_manager = LoginManager()
mail = Mail() 

# --- RATE LIMITER CONFIGURATION ---
# Increased limits to prevent "429 Too Many Requests" errors
# "2000 per hour" allows for ~1 request every 2 seconds, which covers dashboard polling.
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["10000 per day", "2000 per hour"],
    storage_uri="memory://" 
)