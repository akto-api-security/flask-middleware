import os
from flask import Flask, request, jsonify, send_file, Response
from middleware import setup_middleware

app = Flask(__name__)

setup_middleware(app)

# GET endpoint
@app.route('/get', methods=['GET'])
def get_endpoint():
    response_data = {
        "message": "This is a GET request",
        "status": "success"
    }
    return jsonify(response_data)


# POST endpoint
@app.route('/post', methods=['POST'])
def post_endpoint():
    data = request.json
    return jsonify(data)


# File upload endpoint
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"msg": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"msg": "No selected file"}), 400

    if file:
        filename = file.filename
        file.save(os.path.join('uploads', filename))
        return jsonify({"msg": "success"}), 200


@app.route('/download', methods=['GET'])
@app.route('/download/<filename>', methods=['GET'])
def download_file(filename=None):
    if filename:
        file_path = os.path.join('uploads', filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            return "File not found", 404
    else:
        files = os.listdir('uploads')
        if not files:
            return jsonify({"message": "No files found"}), 404
        return jsonify({"files": files})


if __name__ == '__main__':
    # Ensure the upload directory exists
    os.makedirs('uploads', exist_ok=True)
    app.run(debug=True)
