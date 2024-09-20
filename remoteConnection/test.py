from flask import Flask
import psycopg2

app = Flask(__name__)

@app.route("/")
def hello():
    output = []
    output.append("Hello Try2<br>")

    try:
        output.append("Starting DB connection...<br>")
        connection = psycopg2.connect(
            host="database-feature2.cf2umm2mcxhw.us-east-1.rds.amazonaws.com",
            database="database_feature2",
            user="postgres",
            password="o080299P!",
            port="5432"
        )
        output.append("Connected to the database.<br>")
        cursor = connection.cursor()

        # Insert a player
        output.append("Inserting player with playerID=0 and playerName='Cehck'<br>")
        cursor.execute("INSERT INTO players (playerID, playerName) VALUES (%s, %s) ON CONFLICT (playerID) DO NOTHING;", (0, "Cehck"))

        # Commit the transaction
        connection.commit()
        output.append("Insert committed.<br>")

        # Fetch data
        output.append("Fetching player with playerID=0<br>")
        cursor.execute("SELECT * FROM players WHERE playerID=%s;", (0,))
        row = cursor.fetchone()

        # Display the data
        output.append(f"Fetched row from players table: {row}<br>")

        cursor.close()
        connection.close()
        output.append("Connection closed.<br>")

    except Exception as e:
        output.append(f"Error: {e}<br>")

    # Join all the output lines into a single string and return it
    return "".join(output)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
