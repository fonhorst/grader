import logging
import os

from celery import Celery

logger = logging.getLogger(__name__)


BATCH_TASKS_QUEUE = "batch_tasks"
KUBERNETES_BATCH_TASKS_QUEUE = "kubernetes_batch_tasks"


def make_app():
    app = Celery(
        'grader',
        broker=os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672"),
        backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
    )

    # these settings may be important for setting them during setup
    # task_acks_late=False,
    # task_reject_on_worker_lost=True,
    # task_acks_on_failure_or_timeout=False,

    # for explanation of this options see:
    # 1. https://stackoverflow.com/questions/56805193/how-to-set-up-celery-producer-send-task-timeout
    # 2. https://github.com/celery/celery/issues/4627#issuecomment-396907957
    app.conf.update(
        task_default_queue='batch_tasks',
        task_default_exchange='batch_tasks',
        task_default_exchange_type='topic',
        task_default_routing_key='batch_tasks.#',
        worker_prefetch_multiplier=1,
        broker_transport_options={
            "max_retries": 3,
            "interval_start": 0,
            "interval_step": 0.2,
            "interval_max": 0.5
        }
    )

    return app
