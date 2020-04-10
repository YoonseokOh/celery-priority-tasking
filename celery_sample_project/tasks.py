from celery import Celery, shared_task
from kombu import Exchange, Queue
import time

# Celery
celery_app = Celery()
celeryconfig = {}
# redis
# celeryconfig['CELERY_RESULT_BACKEND'] = 'redis://localhost'
# rabbitmq
celeryconfig['CELERY_BROKER_URL'] = 'amqp://guest:guest@localhost//'
celeryconfig['CELERY_TASK_DEFAULT_QUEUE'] = 'priority_queue'
celeryconfig['CELERY_QUEUES'] = (
    Queue('priority_queue', Exchange('priority_queue'), routing_key='priority_queue',
          queue_arguments={'x-max-priority': 10}),
)
# True : get tasks after previous working is done / False : get work during others task is running
celeryconfig['CELERY_ACKS_LATE'] = True
# The max number of prefetch queue in each worker. DEFAULT : 4 / 1 is good for priority queue.
celeryconfig['CELERYD_PREFETCH_MULTIPLIER'] = 1
celery_app.config_from_object(celeryconfig)

# celery worker -c 1 -A tasks -Q tasks --loglevel=info
# ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill
# amqp://jm-user1:sample@localhost/jm-vhost

# rabbitmqctl stop
# rabbitmq-server -detached

###

# Idea of the sample project is to simulate transcoding of a video into
# different dimensions using celery priority task queues, and actually
# test if it is priority based.

# example: transcode_360p.apply_async(queue='tasks', priority=1)

###


@celery_app.task(queue='priority_queue')
def transcode_360p():
    # Simulate transcoding a video into 360p resolution
    print('BEGIN:   Video transcoding to 360p resolution!')
    time.sleep(1)
    print('END:   Video transcoded to 360p resolution!')
    return "360p"


@celery_app.task(queue='priority_queue')
def transcode_480p():
    # Simulate transcoding a video into 480p resolution
    print('BEGIN:   Video transcoding to 480p resolution!')
    time.sleep(2)
    print('END:   Video transcoded to 480p resolution!')
    return "480p"


@celery_app.task(queue='priority_queue')
def transcode_720p():
    # Simulate transcoding a video into 720p resolution
    print('BEGIN:   Video transcoding to 720p resolution!')
    time.sleep(3)
    print('END:   Video transcoded to 720p resolution!')
    return "720p"


@celery_app.task(queue='priority_queue')
def transcode_1080p():
    # Simulate transcoding a video into 1080p resolution
    print('BEGIN:   Video transcoding to 1080p resolution!')
    time.sleep(4)
    print('END:   Video transcoded to 1080p resolution!')
    return "1080p"


@celery_app.task(queue='priority_queue')
def common_setup():
    # Setting up the processor
    print('BEGIN:   Setup the processor!')
    time.sleep(5)
    print('END:   Seting up finished')
    return "common_setup"


@celery_app.task(queue='priority_queue')
def test_priority(sleeptime=1, priority=1):
    print(f'BEGIN : priority={priority} sleeptime={sleeptime}')
    time.sleep(sleeptime)
    print(f'END   : priority={priority} sleeptime={sleeptime}')
    return


@celery_app.task(queue='priority_queue')
def end_processing():
    # Method which deletes queue files and ends processing
    print('BEGIN:   Ending the processors!')
    time.sleep(5)
    print('END:    Processing ended')
    return "end_processing"
