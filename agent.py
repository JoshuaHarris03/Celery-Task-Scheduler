# reqirements: pip install celery redis

from celery import Celery
import time
import logging

# Configure Celery app with RabbitMQ broker
app = Celery('task_scheduler', broker='amqp://guest@localhost//', backend='rpc://')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define a complex task with retries and logging
@app.task(bind=True, max_retries=3)
def process_job(self, job_id, data):
    try:
        logger.info(f"Processing job {job_id} with data: {data}")
        # Simulate heavy computation
        time.sleep(5) # Advanced: Could integrate ML inference here.
        result = sum(data) * 2 # Example computation
        logger.info(f"Job {job_id} completed with result: {result}")
        return result
    except Exception as exc:
        logger.error(f"Job {job_id} failed: {exc}")
        self.retry(exc=exc, countdown=10) # Exponential backoff

# Advanced chaining of tasks
def schedule_workflow(job_id, initial_data):
    chain = process_job.s(job_id, initial_data) | process_job.s(job_id + 1, [])
    return chain.apply_async()

if __name__ == '__main__':
    # Example usage: Schedule multiple jobs
    jobs = []
    for i in range(5):
        data = list(range(100 * i (i + 1)))
        result = process_job.delay(i, data)
        jobs.append(result)

        # Wait and collect results with timeout
        for job in jobs:
            try:
                print(f"Result: {job.get(timeout=60)}")
            except TimeoutError:
                print("Job timed out")
        
        # Monitor workers (in production, use Flower Dashboard)
        logger.info("Task scheduler running. Start workers with celery -A task_scheduler worker --loglevel=info ")