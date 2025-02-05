from flask import Flask, request
import os

app = Flask(__name__)

# Save locally on EC2 currently
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# use post method
@app.route("/", methods=["POST"])
def upload_file():
    if "inputFile" not in request.files:
        return {"error": "No file part"}, 400

    file = request.files["inputFile"]
    if file.filename == "":
        return {"error": "No selected file"}, 400

    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    return {"message": f"File saved to {file_path}"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
