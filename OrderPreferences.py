import boto3
import datetime
import json
import traceback

cust_table_name = 'customer-preferences-dev'
order_table_name = 'order-preferences-dev'
keyDelim = '|'

VALIDATION_MSG_RETAILER_MONIKER = 'Invalid input: missing required [retailer] parameter in customer_preferences!'
VALIDATION_MSG_CUSTOMER_ID = 'Invalid input: missing required [customer_id] parameter!'
VALIDATION_MSG_ORDER_ID = 'Invalid input: missing required [order_id] parameter!'
VALIDATION_MSG_STALE_DATA_MODIFICATION = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to [modified_datetime] field in order_preferences in the response for correct value to use.'
VALIDATION_MSG_ORDER_PREFS = 'Invalid input: missing required [order_preferences] section parameter in request body.'
VALIDATION_MSG_POST_DATA_EXISTS = 'Invalid input: you are trying to submit a POST request on an existing order_preference. Use PUT instead.'
VALIDATION_MSG_PUT_DATA_NOT_EXISTS = 'Invalid input: you are tyring to submit a PUT request on an order_preference that does not exist. Use POST instead.'

class InputValidationException(Exception):
    # Server Validation Exception
    responseCode = 400
    response_messages = []
    def __init__(self, response_messages):
        Exception.__init__(self)
        self.response_messages = response_messages

class ResponseMessage:
    def __init__(self, level, code, field, message):
        self.level = level
        self.code = code
        self.field = field
        self.message = message

def method_get(event, context):
    # TODO:
    print("get")

def method_post_put(event, context):
    exception = None
    order_preferences = []

    try:
        error_msgs = []
        retailer_moniker = event['pathParameters']['retailer_moniker']
        customer_id = event['pathParameters']['customer_id']
        order_id = event['pathParameters']['order_id']
        if not retailer_moniker:
            error_msgs.append(ResponseMessage('ERROR', None, 'retailer', VALIDATION_MSG_RETAILER_MONIKER))
        if not customer_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_id', VALIDATION_MSG_CUSTOMER_ID))
        if not order_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'order_id', VALIDATION_MSG_ORDER_ID))
        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)

        if not event or not event.get('body'):
            error_msgs.append(ResponseMessage('ERROR', None, 'order_preferences', VALIDATION_MSG_ORDER_PREFS))
            raise InputValidationException(error_msgs)

        order_prefs = json.loads(event.get('body')).get(order_id)
        if not order_prefs:
            error_msgs.append(ResponseMessage('ERROR', None, 'order_preferences', VALIDATION_MSG_ORDER_PREFS))
            raise InputValidationException(error_msgs)

        __validateOrderPreferences(order_prefs)
    except Exception as e:
        exception = e
        # TODO: Log exception here.

    if exception != None:
        return __makeResponse(order_preferences, exception, None)
    # END OF: Basic input validation

    input = {}
    db = boto3.client('dynamodb');
    try:
        __saveOrderPreference(db, retailer_moniker, customer_id, order_id, order_prefs, event.get('httpMethod'))
    except Exception as e:
        exception = e # Preserve exception for later response.
        # TODO: Log exception here.

    id = __makeKey(retailer_moniker, customer_id, order_id)

    try:
        search_result = __findOrderPreference(db, id)
    except Exception as e:
        exception = e

    order_preferences.append(__makeOrderDetails(search_result))

    return __makeResponse(order_preferences, exception, None)

def __saveOrderPreference(db, retailer_moniker, customer_id, order_id, order_preferences, httpMethod):
    error_msgs = []

    id = __makeKey(retailer_moniker, customer_id, order_id)

    modified_datetime = order_preferences.get('modified_datetime')
    if not modified_datetime:
        modified_datetime = datetime.datetime.utcnow().isoformat()

    created_datetime = modified_datetime

    # Find if the record already exists and perform some checks:
    dbRec = __findOrderPreference(db, id)

    if(httpMethod == 'POST' and dbRec):
        error_msgs.append(ResponseMessage('ERROR', None, None, VALIDATION_MSG_POST_DATA_EXISTS))
        raise InputValidationException(error_msgs)
    elif(httpMethod == 'PUT' and not dbRec):
        error_msgs.append(ResponseMessage('ERROR', None, None, VALIDATION_MSG_PUT_DATA_NOT_EXISTS))
        raise InputValidationException(error_msgs)

    if(dbRec):
        # Overwrite retailer_moniker, customer_id and created_datetime values in the request (modification of these is not allowed):
        retailer_moniker = dbRec.get('retailer_moniker')
        customer_id = dbRec.get('customer_id')
        order_id = dbRec.get('order_id')
        created_datetime = dbRec.get('created_datetime')

        if dbRec.get('modified_datetime') != modified_datetime:
            error_msgs = []
            error_msgs.append(ResponseMessage('ERROR', None, 'modified_datetime', VALIDATION_MSG_STALE_DATA_MODIFICATION))
            raise InputValidationException(error_msgs)
        else:
            modified_datetime = datetime.datetime.utcnow().isoformat()


    __save(db, id, retailer_moniker, customer_id, order_id, order_preferences, created_datetime, modified_datetime)

def __save(db, id, retailer_moniker, customer_id, order_id, order_preferences, created_datetime, modified_datetime):

    # Make a string version of JSON to store:
    order_pref_json = json.dumps(order_preferences)

    response = db.put_item(
        TableName = order_table_name,
        Item={
            'id': {'S': id},
            'retailer_moniker': {'S': retailer_moniker},
            'customer_id': {'S': customer_id},
            'order_id': {'S': order_id},
            'order_pref_json': {'S': order_pref_json},
            'created_datetime': {'S': created_datetime},
            'modified_datetime': {'S': modified_datetime},
        },
        ReturnValues='NONE'
    )

def __findOrderPreference(db, key):

    response = db.get_item(
        TableName=order_table_name,
        Key={'id': {'S': key}},
        ConsistentRead=True,
        ReturnConsumedCapacity='NONE',
        AttributesToGet=[
            'id',
            'retailer_moniker',
            'customer_id',
            'order_id',
            'order_pref_json',
            'created_datetime',
            'modified_datetime'
        ]
    )

    result = __populateRecordFromDynamoDB(response)

    return result

def __findAllOrderPreferences(db, retailer_moniker, customer_id, is_active):

    response = db.scan(
        TableName=order_table_name,
        IndexName='index_name',
        ProjectionExpression=[
            'id',
            'retailer_moniker',
            'customer_id',
            'order_id',
            'locale',
            'order_pref_json',
            'created_datetime',
            'modified_datetime'
        ],
        Limit=100,
        Select='SPECIFIC_ATTRIBUTES',
        FilterExpression='EQ',

    )

def __populateRecordFromDynamoDB(response):
    result = {}

    if response == None or response.get('Item') == None:
        return result

    item = response.get('Item')
    if item.get('id') != None:
        result['id'] = response.get('Item').get('id').get('S')
    if item.get('retailer_moniker') != None:
        result['retailer_moniker'] = response.get('Item').get('retailer_moniker').get('S')
    if item.get('customer_id') != None:
        result['customer_id'] = response.get('Item').get('customer_id').get('S')
    if item.get('order_id') != None:
        result['order_id'] = response.get('Item').get('order_id').get('S')
    if item.get('created_datetime') != None:
        result['created_datetime'] = response.get('Item').get('created_datetime').get('S')
    if item.get('modified_datetime') != None:
        result['modified_datetime'] = response.get('Item').get('modified_datetime').get('S')
    if item.get('order_pref_json') != None:
        result['order_pref_json'] = response.get('Item').get('order_pref_json').get('S')

    return result

def __makeOrderDetails(response):
    result = {}

    if response == None:
        return result

    if response.get('order_pref_json') != None:
        order_pref_json = json.loads(response.get('order_pref_json'))

        order_pref = {}

        order_pref['first_name'] = order_pref_json.get('first_name')
        order_pref['last_name'] = order_pref_json.get('last_name')
        order_pref['locale'] = order_pref_json.get('locale')
        order_pref['address'] = order_pref_json.get('address')
        order_pref['notification_pref'] = order_pref_json.get('notification_pref')
        order_pref['notification_pref_details'] = order_pref_json.get('notification_pref_details')
        order_pref['modified_datetime'] = response.get('modified_datetime')

        result[response.get('order_id')] = order_pref

    return result

def __makeKey(retailer_moniker, customer_id, order_id):
    return ''.join([retailer_moniker, keyDelim, customer_id, keyDelim, order_id])

def __makeResponse(order_preferences, exception, event):
    status = 'success'
    status_code = 200

    body = {}
    body['status'] = status
    body['status_code'] = status_code
    body['messages'] = []

    if exception:
        status = 'error'
        status_code = 400
        body['status'] = status

        if type(exception) == InputValidationException:
            status_code = exception.responseCode
            body['status_code'] = status_code
            for msg in exception.response_messages:
                message = {}
                message['level'] = msg.level
                message['code'] = msg.code
                message['field'] = msg.field
                message['message'] = msg.message
                body['messages'].append(message)
        else:
            status_code = 500
            body['status_code'] = status_code
            message = {}
            message['level'] = 'ERROR'
            message['code'] = None
            message['field'] = None
            message['message'] = exception.args[0]
            body['messages'].append(message)

    body['order_preferences'] = order_preferences
    if(event):
        body['input'] = event

    response = {
        #"statusCode": status_code,
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

# TODO: implement.
def __validateOrderPreferences(order_preferences_dict):
    error_msgs = []

    if len(error_msgs) > 0:
        raise InputValidationException(error_msgs)
