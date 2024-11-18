import json
import random
from faker import Faker
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import os

logging_client = LoggingClient()
logging_client.setup_logging()
logger = logging_client.logger('transaction_data_generator')

fake = Faker()
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()


def get_customer_ids():
    logger.log_text("Pulling customer IDs from Pub/Sub subscription")
    response = subscriber.pull(
        request={
            "subscription": os.environ.get('CUSTOMER_IDS_SUB_PATH'),
            "max_messages": 1
        }
    )

    if not response.received_messages:
        raise Exception("No customer IDs available")

    message = response.received_messages[0]
    customer_ids = json.loads(message.message.data.decode('utf-8'))

    subscriber.acknowledge(
        request={
            'subscription': os.environ.get('CUSTOMER_IDS_SUB_PATH'),
            'ack_ids': [message.ack_id]
        }
    )

    return customer_ids


def generate_transactions(request):
    logger.log_text("Starting transaction generation")
    try:
        num_transactions = int(request.args.get('num_transactions', 1000))
        customer_ids = get_customer_ids()

        transactions = []
        for _ in range(num_transactions):
            amount = round(random.uniform(10.0, 10_000.0), 2)
            installment = random.choice([True, False])

            transaction = {
                'currency': random.choice(['GEL', 'EUR', 'USD']),
                'transaction_id': fake.uuid4(),
                'timestamp': fake.date_time_this_year().strftime(
                    '%Y-%m-%d %H:%M:%S'),
                'amount': amount,
                'total_amount': round(amount + random.uniform(0.0, 10.0), 2),
                'status': random.choice(['completed', 'pending', 'failed']),
                'customer_id': random.choice(customer_ids),
                'merchant_id': fake.uuid4(),
                'city': fake.city(),
                'deposit_method': random.choice(
                    ['card', 'bank_transfer', 'cash']),
                'installment': installment,
                'installment_amount': round(random.uniform(10.0, 50.0),
                                            2) if installment else 0.0,
                'bank_interest': round(random.uniform(1.0, 10.0),
                                       2) if installment else 0.0
            }

            transactions.append(transaction)

        logger.log_text(f'Transactions: {transactions[0]}')
        for transaction in transactions:
            publisher.publish(
                topic=os.environ.get('TRANSACTION_DATA_TOPIC_PATH'),
                data=json.dumps(transaction).encode('utf-8')
            )
        logger.log_text("Transaction data published to Pub/Sub topic")

        return {
            'success': True,
            'message': f'Generated {num_transactions} transactions'
        }

    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }, 500
