import boto3
import datetime
import json
import traceback

cust_table_name = 'customer-preferences-dev'
keyDelim = '|'

NOTIFICATION_PREF_SMS = 'SMS'
NOTIFICATION_PREF_FB = 'FB'
NOTIFICATION_PREF_EMAIL = 'EMAIL'

VALIDATION_MSG_CUSTOMER_ID = 'Invalid input: missing required [customer_id] parameter!'
VALIDATION_MSG_RETAILER_MONIKER = 'Invalid input: missing required [retailer] parameter in customer_preferences!'
VALIDATION_MSG_STALE_DATA_MODIFICATION = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to [modified_datetime] field in customer_preferences in the response for correct value to use.'
VALIDATION_MSG_CUST_PREFS = 'Invalid input: missing required [customer_preferences] section parameter in request body.'
VALIDATION_MSG_FIRST_NAME = 'Invalid input: missing required parameter [first_name] in customer_preferences.'
VALIDATION_MSG_LAST_NAME = 'Invalid input: missing required parameter [last_name] in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS = 'Invalid input: missing required [notification_pref] section parameter in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS = 'Invalid input: missing required [notification_pref_details] section parameter in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_LENGTH = 'Invalid input: channels in [notification_pref] must match with their [notification_pref_details].'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_ITEM = 'Invalid input: missing customer [notification_pref_details] for: '
VALIDATION_MSG_NOTIFICATION_PREFS_CHANNEL = 'Invalid input: invalid notification preference value. Supported values are: ' + ''.join([NOTIFICATION_PREF_SMS, keyDelim, NOTIFICATION_PREF_FB, keyDelim, NOTIFICATION_PREF_EMAIL])
VALIDATION_MSG_ADDRESS = 'Invalid input: missing required [address] section parameter in customer_preferences.'
VALIDATION_MSG_ADDRESS_STREET1 = 'Invalid input: missing required parameter [street_1] in address.'
VALIDATION_MSG_ADDRESS_CITY = 'Invalid input: missing required parameter [city] in address.'
VALIDATION_MSG_ADDRESS_STATE = 'Invalid input: missing required parameter [state] in address.'
VALIDATION_MSG_ADDRESS_ZIP = 'Invalid input: missing required parameter [zip] in address.'
VALIDATION_MSG_ADDRESS_COUNTRY = 'Invalid input: missing required parameter [country] in address.'
VALIDATION_MSG_POST_DATA_EXISTS = 'Invalid input: you are trying to submit a POST request on an existing customer_preference. Use PUT instead.'
VALIDATION_MSG_PUT_DATA_NOT_EXISTS = 'Invalid input: you are tyring to submit a PUT request on a customer_preference that does not exist. Use POST instead.'

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
    exception = None

    # Basic input validation:
    try:
        error_msgs = []
        retailer_moniker = event['pathParameters']['retailer_moniker']
        customer_id = event['pathParameters']['customer_id']
        if not retailer_moniker or retailer_moniker == 'narvar-speedee':
            error_msgs.append(ResponseMessage('ERROR', None, 'retailer_name', VALIDATION_MSG_RETAILER_MONIKER))
        if not customer_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_id', VALIDATION_MSG_CUSTOMER_ID))
        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)
    except InputValidationException as e:
        exception = e

    customer_preferences = {}
    if exception != None:
        return __makeResponse(customer_preferences, exception, None)
    # END OF: Basic input validation

    customerKey = __makeKey(retailer_moniker, customer_id)

    result = {}
    try:
        db = boto3.client('dynamodb');
        result = __findCustomerPreference(db, customerKey)
        customer_preferences = __makeCustomerDetails(result)
    except Exception as e:
        exception = e # Preserve exception for the response.
        # TODO: Log the exception.

    return __makeResponse(customer_preferences, exception, None)

def method_post_put(event, context):
    exception = None
    customer_preferences = {}

    try:
        error_msgs = []
        retailer_moniker = event['pathParameters']['retailer_moniker']
        customer_id = event['pathParameters']['customer_id']
        if not retailer_moniker:
            error_msgs.append(ResponseMessage('ERROR', None, 'retailer', VALIDATION_MSG_RETAILER_MONIKER))
        if not customer_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_id', VALIDATION_MSG_CUSTOMER_ID))
        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)

        if not event or not event.get('body'):
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', VALIDATION_MSG_CUST_PREFS))
            raise InputValidationException(error_msgs)

        cust_prefs = json.loads(event.get('body')).get('customer_preferences')
        if not cust_prefs:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', VALIDATION_MSG_CUST_PREFS))
            raise InputValidationException(error_msgs)

        __validateCustomerPreferences(cust_prefs)
    except Exception as e:
        exception = e
        # TODO: Log exception here.

    if exception != None:
        return __makeResponse(customer_preferences, exception, None)

    db = boto3.client('dynamodb');
    try:
        __saveCustomerPreference(db, retailer_moniker, customer_id, cust_prefs, event.get('httpMethod'))
    except Exception as e:
        exception = e # Preserve exception for later response.
        # TODO: Log exception here.

    id = __makeKey(retailer_moniker, customer_id)
    search_result = __findCustomerPreference(db, id)
    customer_preferences = __makeCustomerDetails(search_result)

    return __makeResponse(customer_preferences, exception, None)

def __saveCustomerPreference(db, retailer_moniker, customer_id, cust_preferences, http_method):

    id = __makeKey(retailer_moniker, customer_id)

    # Modified_date value may or may not be provided in post (should be overwritten to current timestamp):
    modified_datetime = cust_preferences.get('modified_datetime')
    if not modified_datetime or http_method.casefold() == 'post':
        modified_datetime = datetime.datetime.utcnow().isoformat()

    created_datetime = modified_datetime

    # Find if the record already exists and perform some checks:
    dbRec = __findCustomerPreference(db, id)

    error_msgs = []
    if(http_method.casefold() == 'post' and dbRec):
        error_msgs.append(ResponseMessage('ERROR', None, None, VALIDATION_MSG_POST_DATA_EXISTS))
        raise InputValidationException(error_msgs)
    elif(http_method.casefold() == 'put' and not dbRec):
        error_msgs.append(ResponseMessage('ERROR', None, None, VALIDATION_MSG_PUT_DATA_NOT_EXISTS))
        raise InputValidationException(error_msgs)

    if(dbRec):
        # Overwrite retailer_moniker, customer_id and created_datetime values in the request (modification of these is not allowed):
        retailer_moniker = dbRec.get('retailer_moniker')
        customer_id = dbRec.get('customer_id')
        created_datetime = dbRec.get('created_datetime')

        if dbRec.get('modified_datetime') != modified_datetime:
            error_msgs = []
            error_msgs.append(ResponseMessage('ERROR', None, 'modified_datetime', VALIDATION_MSG_STALE_DATA_MODIFICATION))
            raise InputValidationException(error_msgs)
        else:
            modified_datetime = datetime.datetime.utcnow().isoformat()

    # Strip out modified_datetime from cust_pref_json input (it is there only for the client display):
    cust_preferences.pop('modified_datetime', None)

    __save(db, id, retailer_moniker, customer_id, cust_preferences, created_datetime, modified_datetime)

def __save(db, id, retailer_moniker, customer_id, cust_preferences, created_datetime, modified_datetime):

    # Make a string version of JSON to store:
    customer_pref_json = json.dumps(cust_preferences)

    response = db.put_item(
        TableName = cust_table_name,
        Item={
            'id': {'S': id},
            'retailer_moniker': {'S': retailer_moniker},
            'customer_id': {'S': customer_id},
            'customer_pref_json': {'S': customer_pref_json},
            'created_datetime': {'S': created_datetime},
            'modified_datetime': {'S': modified_datetime},
        },
        ReturnValues='NONE'
    )

def __findCustomerPreference(db, key):

    response = db.get_item(
        TableName=cust_table_name,
        Key={'id': {'S': key}},
        ConsistentRead=True,
        ReturnConsumedCapacity='NONE',
        AttributesToGet=[
            'id',
            'retailer_moniker',
            'customer_id',
            'customer_pref_json',
            'created_datetime',
            'modified_datetime'
        ]
    )

    result = __populateRecordFromDynamoDB(response)

    return result

def __validateCustomerPreferences(cust_preferences):
    error_msgs = []

    # No point in checking further if this check fails:
    if not cust_preferences:
        error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', VALIDATION_MSG_CUST_PREFS))
        raise InputValidationException(error_msgs)

    if(not cust_preferences.get('first_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'first_name', VALIDATION_MSG_FIRST_NAME))
    if(not cust_preferences.get('last_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'last_name', VALIDATION_MSG_LAST_NAME))

    notification_preferences = cust_preferences.get('notification_pref')
    notification_preferences_details = cust_preferences.get('notification_pref_details')

    # Notification Preference and Notification Preferences Details lists cannot be null (but can be empty)
    if(notification_preferences == None):
        error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref', VALIDATION_MSG_NOTIFICATION_PREFS))
        raise InputValidationException(error_msgs)
    else:
        notification_preferences = sorted(notification_preferences)

    if(notification_preferences_details == None):
        error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref_details', VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS))
        raise InputValidationException(error_msgs)
    notification_pref_details_sorted = sorted(notification_preferences_details, key=lambda k: k['name'])

    # Notification Preferences and Notification Preferences Details must be of the same length:
    if len(notification_preferences) != len(notification_pref_details_sorted):
        error_msgs.append(ResponseMessage('ERROR', None, None, VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_LENGTH))
        raise InputValidationException(error_msgs)

    # Loop through the channels and verify that the corresponding item is present in details:
    idx = 0
    for item in notification_preferences:
        # Check for allowed value options:
        if not item or not (item == NOTIFICATION_PREF_SMS or item == NOTIFICATION_PREF_FB or item == NOTIFICATION_PREF_EMAIL):
            error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref', VALIDATION_MSG_NOTIFICATION_PREFS_CHANNEL))

        # Make sure that there is a corresponding option for Notification Preferences Details:
        details = notification_pref_details_sorted[idx]
        if not details or item != details.get('name') or not details.get('value'):
            error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref_details', VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_ITEM + item))

        idx += 1

    # Validate address fields:
    address = cust_preferences.get('address')
    if(not address):
        error_msgs.append(ResponseMessage('ERROR', None, 'address', VALIDATION_MSG_ADDRESS))
        raise InputValidationException(error_msgs)
    if not address.get('street_1'):
        error_msgs.append(ResponseMessage('ERROR', None, 'street_1', VALIDATION_MSG_ADDRESS_STREET1))
    if not address.get('city'):
        error_msgs.append(ResponseMessage('ERROR', None, 'city', VALIDATION_MSG_ADDRESS_CITY))
    if not address.get('state'):
        error_msgs.append(ResponseMessage('ERROR', None, 'state', VALIDATION_MSG_ADDRESS_STATE))
    if not address.get('zip'):
        error_msgs.append(ResponseMessage('ERROR', None, 'zip', VALIDATION_MSG_ADDRESS_ZIP))
    if not address.get('country'):
        error_msgs.append(ResponseMessage('ERROR', None, 'country', VALIDATION_MSG_ADDRESS_COUNTRY))

    if len(error_msgs) > 0:
        raise InputValidationException(error_msgs)

    # Default locale if missing:
    if not cust_preferences.get('locale'):
        cust_preferences['locale'] = 'en_US'

def __makeKey(retailer_moniker, customer_id):
    return ''.join([retailer_moniker, keyDelim, customer_id])

def __makeCustomerDetails(response):
    result = {}

    if response == None:
        return result

    if response.get('customer_pref_json') != None:
        customer_pref_json = json.loads(response.get('customer_pref_json'))

        result['first_name'] = customer_pref_json.get('first_name')
        result['last_name'] = customer_pref_json.get('last_name')
        result['locale'] = customer_pref_json.get('locale')
        result['address'] = customer_pref_json.get('address')
        result['notification_pref'] = customer_pref_json.get('notification_pref')
        result['notification_pref_details'] = customer_pref_json.get('notification_pref_details')
        result['modified_datetime'] = response.get('modified_datetime')

    return result

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
    if item.get('created_datetime') != None:
        result['created_datetime'] = response.get('Item').get('created_datetime').get('S')
    if item.get('modified_datetime') != None:
        result['modified_datetime'] = response.get('Item').get('modified_datetime').get('S')
    if item.get('customer_pref_json') != None:
        customer_pref = json.loads(response.get('Item').get('customer_pref_json').get('S'))
        # Populate modified_datetime for client display purposes:
        customer_pref['modified_datetime'] = result.get('modified_datetime')
        result['customer_pref_json'] = json.dumps(customer_pref)

    return result

def __makeResponse(customer_preferences, exception, event):
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

    body['customer_preferences'] = customer_preferences
    if(event):
        body['input'] = event

    response = {
        #"statusCode": status_code,
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response
