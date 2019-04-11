from flask import Flask, request, render_template
from tasks import *
from celery import group, chain, chord

app = Flask(__name__)


@app.route('/')
def index():
    return render_template("index.html")


@app.route('/transcode360p', methods=['POST'])
def transcodeTo360p():
    if request.method == 'POST':
        try:
            priority = int(request.args['priority'])
            # transcode_360p.apply_async(queue='tasks', priority=priority)
            transcode_360p.apply_async(priority=priority)
        except:
            # transcode_360p.apply_async()
            # transcode_360p()
            transcode_360p.delay()
        return f'Video is getting transcoded to 360p dimensions!'
    else:
        return 'ERROR: Wrong HTTP Method'


@app.route('/transcode480p', methods=['POST'])
def transcodeTo480p():
    if request.method == 'POST':
        try:
            priority = int(request.args['priority'])
            transcode_480p.apply_async(priority=priority)
        except:
            transcode_480p.delay()
        return 'Video is getting transcoded to 480p dimensions!'
    else:
        return 'ERROR: Wrong HTTP Method'


@app.route('/transcode720p', methods=['POST'])
def transcodeTo720p():
    if request.method == 'POST':
        try:
            priority = int(request.args['priority'])
            transcode_720p.apply_async(priority=priority)
        except:
            transcode_720p.delay()
        return 'Video is getting transcoded to 720p dimensions!'
    else:
        return 'ERROR: Wrong HTTP Method'


@app.route('/transcode1080p', methods=['POST'])
def transcodeTo1080p():
    if request.method == 'POST':
        try:
            priority = int(request.args['priority'])
            transcode_1080p.apply_async(priority=priority)
        except:
            transcode_1080p.delay()
        return 'Video is getting transcoded to 1080p dimensions!'
    else:
        return 'ERROR: Wrong HTTP Method'


@app.route('/transcodeALL', methods=['POST'])
def transcodeToALL():
    if request.method == 'POST':
        # We will do something like this to simulate actual processing of a video
        transcoding_tasks = group(
            transcode_1080p.signature(priority=1, immutable=True),
            transcode_720p.signature(priority=2, immutable=True),
            transcode_480p.signature(priority=3, immutable=True),
            transcode_360p.signature(priority=4, immutable=True)
        )
        chord(transcoding_tasks)(end_processing.signature(immutable=True))
        return 'Video is getting transcoded to all dimensions!'
    else:
        return 'ERROR: Wrong HTTP Method'


@app.route('/transcodeMANY', methods=['POST'])
def transcodeToMany():
    for i in range(int(request.args['numOfVids'])):
        # Real time scenario
        
        transcoding_tasks = group(
            transcode_1080p.signature(priority=1, immutable=True),
            transcode_720p.signature(priority=2, immutable=True),
            transcode_480p.signature(priority=3, immutable=True),
            transcode_360p.signature(priority=4, immutable=True)
        )
        
        # Removing initial common_setup
        # chord works fine with priorities but not chain :(
        # hence -
        chord(transcoding_tasks)(end_processing.signature(immutable=True))

    return(str(request.args['numOfVids']) + ' video(s) being transcoded to all dimensions!')


if __name__ == "__main__":
    app.run(debug=True)
