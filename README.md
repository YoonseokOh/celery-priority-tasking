# celery-priority-tasking
This is a prototype to schedule jobs in the backend based on some priority using Rabbitmq and Celery.
This is modified code to run with Rabbitmq forked from https://github.com/vijeth-aradhya/celery-priority-tasking

## Video Transcoding Simulation
In this sample project, the powers of Celery and RabbitMQ in prioritized tasking can be seen. The simulation for transcoding a video to 
- 360p resolution takes 1 seconds
- 480p resolution takes 2 seconds
- 720p resolution takes 3 seconds
- 1080p resolution takes 4 seconds

Of course this isn't even close to the real scencario, but it's good enough to observe the prioritized tasking.

## To Enable Celery's Priority Tasking
These are the things to keep in mind to enable priority tasking without getting into trouble or unnecessary errors.
- `'x-max-priority': 10` This argument must be provided to the queue so that RabbitMQ knows that tasks should be prioritized.
- There is a possibility that the queue has no chance to prioritize the messages (because they get downloaded before the sorting happens). Prefetch multiplier is 4 by default.
```
CELERY_ACKS_LATE = True
CELERYD_PREFETCH_MULTIPLIER = 1
```

## Execution
- Make virtual environment `python -m venv venv`
- Changed to virtual environment `. venv/bin/activate`
- Run `pip install -r requirements.txt` and go into the directory `cd python.celery-priority-tasking/celery_sample_project`.
- Run `python main.py` for starting up Flask.
- Then, in another terminal, run `celery worker -c 1 -A tasks -Q tasks --loglevel=info`.
  - One celery worker(-c 1) setting is easy to understanding processing.
- Open up a browser `open http://localhost:5000` and start simulating!

## Preview
There is a small preview video [here](https://youtu.be/nQXO2kjGV9M) with only 1 Celery worker so that it's easy to observe the tasks getting sorted.

## Install dependencies for RabbitMQ
To install dependencies on Debian machines, use the script provided or otherwise modify it accordingly for other operating systems.

## Tutorials
- [Executing tasks](http://docs.celeryproject.org/en/master/userguide/canvas.html) (Official Celery doc)
- [Celery](https://www.fullstackpython.com/task-queues.html) (full stack python)
- [Task Queues](https://www.fullstackpython.com/task-queues.html) (full stack python)
- [How to integrate RabbitMQ with Celery?](http://docs.celeryproject.org/en/latest/getting-started/brokers/rabbitmq.html#broker-rabbitmq)
