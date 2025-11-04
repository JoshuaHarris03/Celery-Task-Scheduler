# reqirements: pip install celery redis

from celery import Celery
import time
import logging

# Configure Celery app with RabbitMQ broker
app = Celery('task_scheduler', broker='amqp://guest@localhost//', backend='rpc://')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)