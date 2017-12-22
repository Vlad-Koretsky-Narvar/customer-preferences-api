import boto3
import datetime
import json
#from operator import itemgetter
import traceback

cust_table_name = 'customer-preferences-dev'
#order_table_name = 'order_preferences'
keyDelim = '|'

NOTIFICATION_PREF_SMS = 'SMS'
NOTIFICATION_PREF_FB = 'FB'
NOTIFICATION_PREF_EMAIL = 'EMAIL'

VALIDATION_MSG_CUSTOMER_ID = 'Invalid input: missing required [customer_id] parameter!'
VALIDATION_MSG_RETAILER_MONIKER = 'Invalid input: missing required [retailer] parameter in customer_preferences!'
VALIDATION_MSG_STALE_DATA_MODIFICATION = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to modified_datetime field in customer_preferences in the response for correct value to use.'
VALIDATION_MSG_CUST_PREFS = 'Invalid input: missing required [customer_preferences] section parameter in request body.'
VALIDATION_MSG_FIRST_NAME = 'Invalid input: missing required parameter [first_name] in customer_preferences.'
VALIDATION_MSG_LAST_NAME = 'Invalid input: missing required parameter [last_name] in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS = 'Invalid input: missing required [notification_pref] section parameter in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS = 'Invalid input: missing required [notification_pref_details] section parameter in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_LENGTH = 'Invalid input: channels in [notification_pref] must match with their [notification_pref_details].'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_ITEM = 'Invalid input: missing customer [notification_pref] details for: '
VALIDATION_MSG_NOTIFICATION_PREFS_CHANNEL = 'Invalid input: invalid notification preference value. Supported values are: ' + ''.join([NOTIFICATION_PREF_SMS, keyDelim, NOTIFICATION_PREF_FB, keyDelim, NOTIFICATION_PREF_EMAIL])
VALIDATION_MSG_ADDRESS = 'Invalid input: missing required [address] section parameter in customer_preferences.'
VALIDATION_MSG_ADDRESS_STREET1 = 'Invalid input: missing required parameter [street_1] in address.'
VALIDATION_MSG_ADDRESS_CITY = 'Invalid input: missing required parameter [city] in address.'
VALIDATION_MSG_ADDRESS_STATE = 'Invalid input: missing required parameter [state] in address.'
VALIDATION_MSG_ADDRESS_ZIP = 'Invalid input: missing required parameter [zip] in address.'
VALIDATION_MSG_ADDRESS_COUNTRY = 'Invalid input: missing required parameter [country] in address.'

class InputValidationException(Exception):
    # Server Validation Exception
    responseCode = 400
    messages = []
    def __init__(self, err_msgs):
        Exception.__init__(self)
        self.messages = err_msgs

def method_get(event, context):
    exception = None

    # Basic input validation:
    try:
        error_msgs = []
        retailer_moniker = event['pathParameters']['retailer_moniker']
        customer_id = event['pathParameters']['customer_id']
        if not retailer_moniker:
            error_msgs.append(VALIDATION_MSG_RETAILER_MONIKER)
        if not customer_id:
            error_msgs.append(VALIDATION_MSG_CUSTOMER_ID)
        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)
    except InputValidationException as ivrme:
        exception = ivrme

    customer_preferences = {}
    if exception != None:
        return makeResponse(customer_preferences, exception, None)
    # END OF: Basic input validation

    customerKey = makeKey(retailer_moniker, customer_id)

    result = {}
    try:
        result = findCustomerPreference(customerKey)
        customer_preferences = makeCustomerDetails(result)
    except Exception as e:
        exception = e # Preserve exception for the response.
        # TODO: Log the exception.

    return makeResponse(customer_preferences, exception, None)

def method_post(event, context):
    exception = None
    customer_preferences = {}

    try:
        error_msgs = []
        retailer_moniker = event['pathParameters']['retailer_moniker']
        customer_id = event['pathParameters']['customer_id']
        if not retailer_moniker:
            error_msgs.append(VALIDATION_MSG_RETAILER_MONIKER)
        if not customer_id:
            error_msgs.append(VALIDATION_MSG_CUSTOMER_ID)
        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)

        if not event or not event.get('body') or not event.get('body'):
            error_msgs.append(VALIDATION_MSG_CUST_PREFS)
            raise InputValidationException(error_msgs)

        cust_prefs = json.loads(event.get('body')).get('customer_preferences')
        validateCustomerPreferences(cust_prefs)
    except Exception as e:
        exception = e
        # TODO: Log exception here.

    if exception != None:
        return makeResponse(customer_preferences, exception, None)

    try:
        saveCustomerPreference(retailer_moniker, customer_id, cust_prefs)
    except Exception as e:
        exception = e # Preserve exception for later response.
        # TODO: Log exception here.

    customerKey = makeKey(retailer_moniker, customer_id)
    search_result = findCustomerPreference(customerKey)
    customer_preferences = makeCustomerDetails(search_result)

    return makeResponse(customer_preferences, exception, None)

def saveCustomerPreference(retailer_moniker, customer_id, cust_preferences):
    db = boto3.client('dynamodb');

    id = makeKey(retailer_moniker, customer_id)

    modified_datetime = cust_preferences['modified_datetime']
    if not modified_datetime:
        modified_datetime = datetime.datetime.utcnow().isoformat()

    created_datetime = modified_datetime

    # Find if the record already exists and perform some checks:
    dbVersion = findCustomerPreference(id)
    if(dbVersion):
        # Overwrite retailer_moniker, customer_id and created_datetime values in the request (modification of these is not allowed):
        retailer_moniker = dbVersion.get('retailer_moniker')
        customer_id = dbVersion.get('customer_id')
        created_datetime = dbVersion.get('created_datetime')

        if dbVersion.get('modified_datetime') != modified_datetime:
            error_msgs = []
            error_msgs.append(VALIDATION_MSG_STALE_DATA_MODIFICATION)
            raise InputValidationException(error_msgs)
        else:
            modified_datetime = datetime.datetime.utcnow().isoformat()

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

def findCustomerPreference(key):
    db = boto3.client('dynamodb');

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

    result = populateRecordFromDynamoDB(response)

    return result

def validateCustomerPreferences(cust_preferences_dict):
    error_msgs = []

    # No point in checking further if this check fails:
    if not cust_preferences_dict:
        error_msgs.append(VALIDATION_MSG_CUST_PREFS)
        raise InputValidationException(error_msgs)

    if(not cust_preferences_dict.get('first_name')):
        error_msgs.append(VALIDATION_MSG_FIRST_NAME)
    if(not cust_preferences_dict.get('last_name')):
        error_msgs.append(VALIDATION_MSG_LAST_NAME)

    notification_preferences = cust_preferences_dict.get('notification_pref')
    notification_preferences_details = cust_preferences_dict.get('notification_pref_details')

    # Notification Preference and Notification Preferences Details lists cannot be null (but can be empty)
    if(notification_preferences == None):
        error_msgs.append(VALIDATION_MSG_NOTIFICATION_PREFS)
        raise InputValidationException(error_msgs)
    else:
        notification_preferences = sorted(notification_preferences)

    if(notification_preferences_details == None):
        error_msgs.append(VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS)
        raise InputValidationException(error_msgs)
    notification_pref_details_sorted = sorted(notification_preferences_details, key=lambda k: k['name'])

    # Notification Preferences and Notification Preferences Details must be of the same length:
    if len(notification_preferences) != len(notification_pref_details_sorted):
        error_msgs.append(VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_LENGTH)

    # Loop through the channels and verify that the corresponding item is present in details:
    idx = 0
    for item in notification_preferences:
        # Check for allowed value options:
        if not item or not (item == NOTIFICATION_PREF_SMS or item == NOTIFICATION_PREF_FB or item == NOTIFICATION_PREF_EMAIL):
            error_msgs.append(VALIDATION_MSG_NOTIFICATION_PREFS_CHANNEL)

        # Make sure that there is a corresponding option for Notification Preferences Details:
        details = notification_pref_details_sorted[idx]
        if not details or item != details.get('name') or not details.get('value'):
            error_msgs.append(VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_ITEM + item)

        idx += 1

    # Validate address fields:
    address = cust_preferences_dict.get('address')
    if(not address):
        error_msgs.append(VALIDATION_MSG_ADDRESS)
        raise InputValidationException(error_msgs)
    if not address.get('street_1'):
        error_msgs.append(VALIDATION_MSG_ADDRESS_STREET1)
    if not address.get('city'):
        error_msgs.append(VALIDATION_MSG_ADDRESS_CITY)
    if not address.get('state'):
        error_msgs.append(VALIDATION_MSG_ADDRESS_STATE)
    if not address.get('zip'):
        error_msgs.append(VALIDATION_MSG_ADDRESS_ZIP)
    if not address.get('country'):
        error_msgs.append(VALIDATION_MSG_ADDRESS_COUNTRY)

    if len(error_msgs) > 0:
        raise InputValidationException(error_msgs)

def makeKey(retailer_moniker, customer_id):
    return ''.join([retailer_moniker, keyDelim, customer_id])

def makeCustomerDetails(response):
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

def populateRecordFromDynamoDB(response):
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
        result['customer_pref_json'] = response.get('Item').get('customer_pref_json').get('S')

    return result

def makeResponse(customer_preferences, exception, event):
    statusCode = 200
    body = {}
    if not exception:
        body['message'] = "API call executed successfully."
    else:
        body['message'] = "API call failed!"
        if type(exception) == InputValidationException:
            statusCode = exception.responseCode
            #messages = []
            #messages.append('zaebali')
            #messages.append('uzhe vse')
            body['exception_messages'] = []
            for msg in exception.messages:
                body['exception_messages'].append(msg)
        else:
            statusCode = 500
            body['exception_messages'] = exception.args[0]
            body['warning_messages'] = 'standard exception'

    body['customer_preferences'] = customer_preferences
    if(event):
        body['input'] = event

    response = {
        "statusCode": statusCode,
        "body": json.dumps(body)
    }
    return response
