import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from stg_loader.repository import StgRepository
from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor

app = Flask(__name__)


# Set up an endpoint to check if the service is up.
# You can access it with a GET request to localhost:5000/health.
# If the response is healthy, the service is up and running.
@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    # Set the logging level to Debug to be able to view debug logs.
    app.logger.setLevel(logging.DEBUG)

    # We initialize the config. For convenience, we moved the logic for getting the values of environment variables into a separate class.
    config = AppConfig()

    # Initialize the message processor.
    # As long as it's empty. It is needed in order to later write the logic for processing messages from Kafka in it.
    proc = StgMessageProcessor(consumer=config.kafka_consumer(), 
								producer=config.kafka_producer(), 
								redis_client=config.redis_client(), 
								stg_repository=StgRepository(config.pg_warehouse_db()),
								batch_size=100, 
								logger=app.logger)

    # Run the processor in the background.
    # BackgroundScheduler will call the run function of our handler (StgMessageProcessor) according to the schedule.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # Let's start the Flask application.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
