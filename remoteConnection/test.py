from flask import Flask

# Create an instance of the Flask class
app = Flask(__name__)

# Define a route for the root URL ("/")
@app.route("/")
def hello():
    return "Hello World!"

# Run the application on host 0.0.0.0 and port 5000
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
