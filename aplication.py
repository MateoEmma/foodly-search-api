import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from api import app
application = app

if __name__ == "__main__":
    application.run()