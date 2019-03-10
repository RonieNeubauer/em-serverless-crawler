import boto3
import requests
import re
import json

from bs4 import BeautifulSoup
from chalice import Chalice
from dynamodb_json import json_util as dynamo_json

app = Chalice(app_name='app')

URL_PREFIX = 'https://www.imoveiscuritiba.com.br'

HEADERS = {
    'accept': 'text/html',
    'accept-encoding': 'deflate',
    'accept-language': 'en-US,en;q=0.9,pt;q=0.8',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
}


def get_from_chache(id):
    """Gets an object by its id from DynamoDB"""

    client = boto3.client('dynamodb')

    response = client.get_item(
        TableName='serverless_crawler_index',
        Key={
            'id': {'N': str(id)}
        }
    )

    try:
        result = dynamo_json.loads(response['Item'])
        result['source'] = 'cache'

    except KeyError as exc:
        result = None

    return result

def store_in_cachce(object):
    """Stores an object by its id on DynamoDB"""

    client = boto3.client('dynamodb')

    response = client.put_item(
        TableName='serverless_crawler_index',
        Item=json.loads(dynamo_json.dumps(object))
    )

    return response

def get_property(id):
    """Requests the data from the page"""

    # Converts the id into a int
    id = int(id)

    # Check cache
    obj = get_from_chache(id)

    if not obj:

        url = '{prefix}/propriedades/-{id}.html'.format(id=id, prefix=URL_PREFIX)

        try:

            content = requests.get(url, headers=HEADERS).text

        except Exception as exc:

            msg = {
                'status': 'Failure',
                'message': str(exc),
            }

            return msg

        soup = BeautifulSoup(content.encode(), 'html.parser')

        features = {
            f.span.text: f.b.text for f in soup.findAll('li', {'class': 'icon-feature'})
        }

        # Location
        location = soup.findAll('h2', {'class': 'title-location'})[0]
        address = location.b.text
        neighborhood, city = location.span.text.replace(',  ', '').split(',')

        # Operation variables
        operation = soup.findAll('div', {'class': 'price-operation'})[0].text
        price = soup.findAll('div', {'class': 'price-items'})[0].span.text

        # Description
        description = soup.findAll('div', {'id': 'verDatosDescripcion'})
        description = ''.join([e.text for e in description])

        obj = {
            'id': id,
            'url': url,
            'features': features,
            'address': address,
            'neighborhood': neighborhood,
            'city': city,
            'operation': operation,
            'price': price,
            'description': description,
        }

        store_in_cachce(obj)

        obj['source'] = 'crawler'

    return obj


def get_property_list(suffix):
    """Gets a list of properties"""

    if not suffix:

        suffix='/apartamentos-curitiba-pr.html'

    url = '{prefix}{suffix}'.format(prefix=URL_PREFIX, suffix=suffix)

    content = requests.get(url, headers=HEADERS).text

    soup = BeautifulSoup(content, 'html.parser')

    # Gets the refs from the adv title
    links = soup.findAll('a', {'class': 'dl-aviso-a'})
    refs = [l.attrs['href'] for l in links]

    # Extracts the ids from the refs
    ids = [int(re.findall(r'([0-9]+)\.html$', r)[0]) for r in refs]

    # Gets the paginator to the next page
    paginator = soup.find('li', {'class': 'pagination-action-next'})
    paginator = paginator.a.attrs['href']

    obj = {
        'ids': ids,
        'paginator': paginator,
        'url': url,
    }

    return obj

@app.route('/crawl')
def crawl():

    # Build the sqs client
    client = boto3.client('sqs')

    queue_url = client.get_queue_url(
        QueueName='serverless_crawler_queue'
    )['QueueUrl']

    # Get request context
    request = app.current_request

    # Read querystring params
    pages = int(request.query_params.get('pages', 1))

    # Set initial page to None
    pag = None

    obj = {}

    # Iter through the listing
    for i in range(pages):

        result = get_property_list(pag)

        ids, url, pag = result['ids'], result['url'], result['paginator']

        obj[url] = {}
        obj[url]['ids'] = ids
        obj[url]['paginator'] = pag

        for id in ids:

            client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({'id': id})
            )

    return obj

@app.on_sqs_message('serverless_crawler_queue', batch_size=1)
def sqs_get_property(event):

    for record in event:

        record = json.loads(record.body)

        get_property(record['id'])



@app.route('/property/{id}', methods=['GET'])
def route_get_property(id):

    return get_property(id)
