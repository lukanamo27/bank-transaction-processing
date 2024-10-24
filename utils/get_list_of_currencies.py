import cloudmersive_currency_api_client
from cloudmersive_currency_api_client.rest import ApiException
from airflow.models import Variable


def get_list_of_currencies() -> list[str]:
    configuration = cloudmersive_currency_api_client.Configuration()

    # 369ef145-3c81-456b-b210-91ce47a4cae3
    # configuration.api_key['Apikey'] = Variable.get('CLOUDMERSIVE_API_KEY')
    configuration.api_key['Apikey'] = '369ef145-3c81-456b-b210-91ce47a4cae3'

    api_instance = cloudmersive_currency_api_client.CurrencyExchangeApi(
        cloudmersive_currency_api_client.ApiClient(configuration))

    try:
        api_response = api_instance.currency_exchange_get_available_currencies()
        iso_currency_codes = [currency.iso_currency_code for currency in
                              api_response.currencies]
        return iso_currency_codes
    except ApiException as e:
        print(f'Error fetching currencies: {e}')
        return []