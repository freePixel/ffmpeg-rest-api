from flask import Flask, request, jsonify
import sqlite3
import uuid
import os
import time
import job
import threading
import ffmpeg
import fsgc
import logging
from werkzeug.utils import secure_filename
import extensions
import job_statistics
import auth

app = Flask(__name__, static_url_path='/api_data', static_folder="/api_data")

app.config['UPLOAD_FOLDER'] = '/api_data'

FILES_FOLDER = os.path.join(app.config['UPLOAD_FOLDER'], "files")

@app.route('/ping', methods=['GET'])
def ping():
    logging.info("hello world this is a logging test")
    return jsonify({"status": "ok"}), 200


@app.route('/statistics', methods=['GET'])
def statistics():
    return jsonify(job_statistics.generateVideoCompressionStatisticsDict()), 200

@app.route('/issue-key', methods=['POST'])
def issueKey():
    json = request.json

    if 'secret' not in json:
        return jsonify({'error': 'Expected "secret" argument'}), 400
 
    secret = json["secret"]
    apikey = auth.issueApiKey(secret)
    if apikey is None:
        return jsonify({'error': 'Invalid secret'}), 401
    
    return jsonify({'apikey': apikey}), 200


@app.route('/upload-file', methods=['POST'])
@auth.requireApiKey
def uploadFile():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400 
    
    file = request.files['file']

    mediaType = file.content_type

    if not extensions.isMediaTypeAllowed(mediaType):
        return jsonify({'message': 'Unexpected media type received'}), 400

    filename = extensions.generateFileNameByMedia(mediaType)
    
    file_path = os.path.join(FILES_FOLDER, filename)
    file.save(file_path)

    return jsonify({'message': 'File uploaded sucessfully', 'file_path': file_path, 'file_name' : filename}), 201


@app.route('/job', methods=['GET'])
@auth.requireApiKey
def getJob():
    jobId = request.args.get('id')
    jobManager = job.getJobManager()
    obj = jobManager.getJobById(jobId)

    if obj is None:
        return jsonify({'error': 'Not found'}), 404
    
    return jsonify(obj.toDict()), 200


@app.route('/active-jobs', methods=['GET'])
@auth.requireApiKey
def getJobs():
    jobManager = job.getJobManager()
    return jsonify(jobManager.getActiveJobList()), 200

@app.route('/schedule-video-compression', methods=['POST'])
@auth.requireApiKey
def scheduleVideoCompression():

    data = request.json

    filename = data["filename"]
    quality = data["quality"]
    framerate = data["framerate"]
    factor = data["factor"]

    ext = extensions.extractExtension(filename)

    if ext != 'mp4':
        return jsonify({'error': 'File format should be mp4'}), 400

    if not os.path.exists(os.path.join(FILES_FOLDER, filename)):
        return jsonify({'error': 'File not found'}), 400

    if not all([filename, quality, framerate, factor]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    if quality not in ['720p', '1080p']:
        return jsonify({'error': 'Invalid quality. Must be 720p or 1080p'}), 400

    try:
        framerate = int(framerate)
        if framerate < 1 or framerate > 60:
            raise ValueError
    except ValueError:
        return jsonify({'error': 'Invalid framerate. Must be between 1 and 60'}), 400

    try:
        factor = int(factor)
        if factor < 10 or factor > 50:
            raise ValueError
    except ValueError:
        return jsonify({'error': 'Invalid factor. Must be between 10 and 50'}), 400

    newJob = job.VideoCompressionJob(
        job.VideoCompressorJobData(
            job.BaseJobData(str(uuid.uuid4()), job.JobState.PENDING, job.JobType.VIDEO_COMPRESSION_JOB, job.datetime.now(), None),
            os.path.join(FILES_FOLDER, filename),
            os.path.join(FILES_FOLDER, extensions.generateFileNameByMedia("video/mp4")),
            quality,
            factor,
            framerate
        )
    )

    job.getJobManager().pushJob(newJob)

    return jsonify(newJob.toDict()), 200


def runJobManager():
    jobManager = job.getJobManager()
    jobManager.runBlocking()

def runFSGC():
    logging.info("Started FSGC")
    while True:
        fsgc.removeInvalidFiles(FILES_FOLDER)
        time.sleep(60 * 60 * 24)

    

if __name__ == '__main__': 
    logging.getLogger('root').setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,
                        format=('%(filename)s: '
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s')
                        )
    logging.getLogger('werkzeug')
    task = threading.Thread(target=runJobManager)
    task.daemon = True
    task.start()

    gcTask = threading.Thread(target=runFSGC)
    gcTask.daemon = True
    gcTask.start()
  

    app.run(debug=True,host="0.0.0.0")