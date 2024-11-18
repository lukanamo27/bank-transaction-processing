import json
import random
from faker import Faker
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import os


logging_client = LoggingClient()
logging_client.setup_logging()
logger = logging_client.logger('customer_data_generator')

fake = Faker()
publisher = pubsub_v1.PublisherClient()


def generate_customers(request):
    if request.method == 'GET' and request.path == '/_ah/health':
        return 'OK', 200

    logger.log_text('Starting customer generation')
    try:
        num_customers = int(request.args.get('num_customers', 100))

        customers = []
        customer_ids = []
        for _ in range(num_customers):
            customer_id = fake.uuid4()
            customer = {
                'customer_id': customer_id,
                'age': random.randint(18, 100),
                'fullname': fake.gen_name(),
                'job': fake.job(),
                'marital': random.choice(['single', 'married', 'divorced']),
                'balance': round(random.uniform(100.0, 100_000.0), 2),
                'housing': random.choice(['yes', 'no']),
                'loan': random.choice([True, False]),
                'contact': fake.phone_number(),
                'gender': random.choice(['male', 'female', 'other']),
                'credit_score': random.randint(300, 850),
                'geography': fake.country(),
                'number_of_products': random.randint(1, 5),
                'tenure': random.randint(1, 10),
                'loan_number': random.randint(0, 3)
            }
            customers.append(customer)
            customer_ids.append(customer_id)

        logger.log_text(f'customer_data: {customers[0]}')
        logger.log_text(f'customer_id: {customer_ids[0]}')

        for customer in customers:
            publisher.publish(
                topic=os.environ.get('CUSTOMER_DATA_TOPIC_PATH'),
                data=json.dumps(customer).encode('utf-8')
            )
        logger.log_text("Customer data published to Pub/Sub topic")

        publisher.publish(
            topic=os.environ.get('CUSTOMER_IDS_TOPIC_PATH'),
            data=json.dumps(customer_ids).encode('utf-8')
        )
        logger.log_text("Customer IDs published to Pub/Sub topic")

        return {
            'success': True,
            'message': f'Generated {num_customers} customers'
        }, 200
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }, 500
