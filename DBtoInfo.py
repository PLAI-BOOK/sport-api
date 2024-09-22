from connection import *
from db import connectDB

# Load environment variables from .env file
conn = connectDB.get_db_connection()  # Connect to the default postgres database

# Create a cursor object
cur = conn.cursor()













# Run the main function
if __name__ == "__main__":
    print("bla")


# Close the cursor and connection
cur.close()
conn.close()