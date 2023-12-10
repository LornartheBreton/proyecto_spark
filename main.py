from flask import Flask, request
import json

app = Flask(__name__)

# This endpoint will receive data from Spark and store it
@app.route('/post_data', methods=['POST'])
def post_data():
    data = request.json
    # Store data - for example, append to a file or insert into a database
    #with open("data.json", "a") as file:
        #json.dump(data, file)
        #file.write("\n")
    print(data)
    return "Data received"

if __name__ == '__main__':
    app.run(port=5000)
