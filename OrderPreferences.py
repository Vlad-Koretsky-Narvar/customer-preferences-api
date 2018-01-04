from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key, Attr
import datetime
import json
import traceback
import test

cust_table_name = 'customer-preferences-dev'
order_table_name = 'order-preferences-dev'
order_table_gsi_name = 'retailer-customer-idx'
keyDelim = '|'

NOTIFICATION_PREF_SMS = 'SMS'
NOTIFICATION_PREF_FB = 'FB'
NOTIFICATION_PREF_EMAIL = 'EMAIL'

VALIDATION_MSG_RETAILER_MONIKER = 'Invalid input: missing required [retailer] parameter in customer_preferences!'
VALIDATION_MSG_CUSTOMER_ID = 'Invalid input: missing required [customer_id] parameter!'
VALIDATION_MSG_ORDER_ID = 'Invalid input: missing required [order_id] parameter!'
VALIDATION_MSG_STALE_DATA_MODIFICATION = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to [modified_datetime] field in order_preferences in the response for correct value to use.'
VALIDATION_MSG_ORDER_PREFS = 'Invalid input: missing required [order_preferences] section parameter in request body.'
VALIDATION_MSG_POST_DATA_EXISTS = 'Invalid input: you are trying to submit a POST request on an existing order_preference. Use PUT instead.'
VALIDATION_MSG_PUT_DATA_NOT_EXISTS = 'Invalid input: you are tyring to submit a PUT request on an order_preference that does not exist. Use POST instead.'
VALIDATION_MSG_ORDER_PREFS = 'Invalid input: missing required [order_preferences] section parameter in request body.'
VALIDATION_MSG_FIRST_NAME = 'Invalid input: missing required parameter [first_name] in order_preferences.'
VALIDATION_MSG_LAST_NAME = 'Invalid input: missing required parameter [last_name] in order_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS = 'Invalid input: missing required [notification_pref] section parameter in order_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS = 'Invalid input: missing required [notification_pref_details] section parameter in order_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_LENGTH = 'Invalid input: channels in [notification_pref] must match with their [notification_pref_details].'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_ITEM = 'Invalid input: missing order [notification_pref_details] for: '
VALIDATION_MSG_NOTIFICATION_PREFS_CHANNEL = 'Invalid input: invalid notification preference value. Supported values are: ' + ''.join([NOTIFICATION_PREF_SMS, keyDelim, NOTIFICATION_PREF_FB, keyDelim, NOTIFICATION_PREF_EMAIL])

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
    order_id = None

    # Basic input validation:
    try:
        error_msgs = []

        retailer_moniker = event.get('pathParameters').get('retailer_moniker')
        customer_id = event.get('pathParameters').get('customer_id')

        if not retailer_moniker:
            error_msgs.append(ResponseMessage('ERROR', None, 'retailer_name', VALIDATION_MSG_RETAILER_MONIKER))
        if not customer_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_id', VALIDATION_MSG_CUSTOMER_ID))
        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)

        is_include_inactive_str = None
        order_id = None
        if event.get('queryStringParameters'):
            is_include_inactive_str = event.get('queryStringParameters').get('is_include_inactive')
            order_id = event.get('queryStringParameters').get('order_id') or None

        if not is_include_inactive_str or is_include_inactive_str.casefold() != 'true':
            is_include_inactive = False
        else:
            is_include_inactive = True

    except InputValidationException as e:
        exception = e

    if exception != None:
        return __makeResponse([], exception, None)
    # END OF: Basic input validation

    try:
        order_preferences = []

        key = __makeKey(retailer_moniker, customer_id, order_id)

        db = boto3.client('dynamodb');
        try:
            if order_id:
                result = {}
                result = __findOrderPreference(db, key)
                order_preferences = __makeOrderDetails(result)
            else:
                result = []
                result = __findAllOrderPreferences(db, key, is_include_inactive)
                order_preferences = __makeOrderDetails(result)
        except Exception as e:
            exception = e # Preserve exception for the response.
            # TODO: Log the exception.
            print('Exception getting order preferences for key: [' + key + ']')
    except Exception as e:
        exception = e

    return __makeResponse(order_preferences, exception, None)

def method_post_put(event, context):
    exception = None
    order_preferences = []

    try:
        error_msgs = []
        retailer_moniker = event.get('pathParameters').get('retailer_moniker')
        customer_id = event.get('pathParameters').get('customer_id')
        order_id = event.get('pathParameters').get('order_id')
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
        print('Exception saving order preferences: [' + json.dumps(error_msgs) + ']')

    if exception != None:
        return __makeResponse(order_preferences, exception, None)
    # END OF: Basic input validation

    input = {}
    db = boto3.client('dynamodb')
    try:

        is_active = True # TODO: hard-coding for now - need to discuss use-cases further!
        __saveOrderPreference(db, retailer_moniker, customer_id, order_id, is_active, order_prefs, event.get('httpMethod'))
    except Exception as e:
        exception = e # Preserve exception for later response.
        # TODO: Log exception here.
        print('Exception saving order preferences: [' + json.dumps(error_msgs) + ']')

    id = __makeKey(retailer_moniker, customer_id, order_id)

    try:
        search_result = __findOrderPreference(db, id)
    except Exception as e:
        exception = e

    order_preferences = __makeOrderDetails(search_result)

    return __makeResponse(order_preferences, exception, None)

def __saveOrderPreference(db, retailer_moniker, customer_id, order_id, is_active, order_preferences, httpMethod):
    error_msgs = []

    id = __makeKey(retailer_moniker, customer_id, order_id)

    modified_datetime = order_preferences.get('modified_datetime')
    if not modified_datetime:
        modified_datetime = datetime.datetime.utcnow().isoformat()

    created_datetime = modified_datetime

    # Find if the record already exists and perform some checks:
    dbRecs = __findOrderPreference(db, id)
    dbRec = None if not dbRecs else dbRecs[0]
    #dbRec = __findOrderPreference(db, id)[0] or None
    print('VLAD: __findOrderPreference call executed successfully.')

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
            # Strip out modified_datetime and is_active from order_pref_json input (it is there only for the client display):
            order_preferences.pop('modified_datetime', None)
            order_preferences.pop('is_active', None)


    __save(db, id, retailer_moniker, customer_id, order_id, is_active, order_preferences, created_datetime, modified_datetime)

def __save(db, id, retailer_moniker, customer_id, order_id, is_active, order_preferences, created_datetime, modified_datetime):

    scan_id = __makeKey(retailer_moniker, customer_id, None)

    # Make a string version of JSON to store:
    order_pref_json = json.dumps(order_preferences)

    response = db.put_item(
        TableName = order_table_name,
        Item={
            'id': {'S': id},
            'scan_id': {'S': scan_id},
            'retailer_moniker': {'S': retailer_moniker},
            'customer_id': {'S': customer_id},
            'order_id': {'S': order_id},
            #'is_guest': {'BOOL': is_guest },
            'is_active': {'BOOL': is_active },
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
            'is_guest',
            'is_active',
            'order_pref_json',
            'created_datetime',
            'modified_datetime'
        ]
    )

    results = []
    result = {}
    if not response or not response.get('Item'):
        return result

    result = __populateRecordFromDynamoDB(response.get('Item'))
    results.append(result)

    return results

def __findAllOrderPreferences(db, key, is_include_inactive):

    # Only look for records with is_active = True if is_include_inactive == False
    filter_expression = ''
    exp_attr_values = {}
    if not is_include_inactive:
        expression_attribute_values = {
            ':scan_id': { 'S': key },
            ':is_active_true': {'BOOL': True},
        }
        filter_expression = 'is_active = :is_active_true'
    else:
        expression_attribute_values = {
            ':scan_id': { 'S': key },
            ':is_active_true': {'BOOL': True},
            ':is_active_false': {'BOOL': False},
        }
        filter_expression = 'is_active in (:is_active_true, :is_active_false)'

    response = db.query(
        TableName=order_table_name,
        IndexName=order_table_gsi_name,
        Limit=100,
        Select='ALL_PROJECTED_ATTRIBUTES',
        ExpressionAttributeValues=expression_attribute_values,
        KeyConditionExpression='scan_id = :scan_id',
        FilterExpression=filter_expression,
    )

    # TODO: take this out later!
    # Debugging section (trouble-shooting an issue where I get 'internal error' response - most likely some issue with the way table and GSI is creaated).
    try:
        msg = type(response)
    except Exception as e:
        error_msgs = []
        error_msgs.append(ResponseMessage('WARN', None, None, e.args[0] + ': ' + msg))
        raise InputValidationException(error_msgs)
    # End of debugging

    results = []
    items = []
    if not response:
        return results

    items = response.get('Items')
    if not items:
        return results

    for item in items:
        result = __populateRecordFromDynamoDB(item)
        results.append(result)

    return results

def __populateRecordFromDynamoDB(item):
    result = {}

    if item == None:
        return result

    if item.get('id') != None:
        result['id'] = item.get('id').get('S')
    if item.get('retailer_moniker') != None:
        result['retailer_moniker'] = item.get('retailer_moniker').get('S')
    if item.get('customer_id') != None:
        result['customer_id'] = item.get('customer_id').get('S')
    if item.get('order_id') != None:
        result['order_id'] = item.get('order_id').get('S')
    if item.get('is_active') != None:
        result['is_active'] = item.get('is_active').get('BOOL')
    if item.get('created_datetime') != None:
        result['created_datetime'] = item.get('created_datetime').get('S')
    if item.get('modified_datetime') != None:
        result['modified_datetime'] = item.get('modified_datetime').get('S')
    if item.get('order_pref_json') != None:
        order_pref = json.loads(item.get('order_pref_json').get('S'))
        # Populate modified_datetime for client display purposes:
        order_pref['modified_datetime'] = result.get('modified_datetime')
        result['order_pref_json'] = json.dumps(order_pref)

    return result

def __makeOrderDetails(response):
    results = []

    if response == None:
        return results

    for item in response:
        if item.get('order_pref_json') != None:
            result = {}
            order_pref_json = json.loads(item.get('order_pref_json'))

            order_pref = {}

            order_pref['first_name'] = order_pref_json.get('first_name')
            order_pref['last_name'] = order_pref_json.get('last_name')
            order_pref['locale'] = order_pref_json.get('locale')
            order_pref['is_guest'] = order_pref_json.get('is_guest')
            order_pref['is_active'] = item.get('is_active')
            order_pref['notification_pref'] = order_pref_json.get('notification_pref')
            order_pref['notification_pref_details'] = order_pref_json.get('notification_pref_details')
            order_pref['modified_datetime'] = item.get('modified_datetime')

            result[item.get('order_id')] = order_pref
            results.append(result)

    return results

def __makeKey(retailer_moniker, customer_id, order_id):
    key = ''.join([retailer_moniker, keyDelim, customer_id])
    if order_id:
        key = ''.join([key, keyDelim, order_id])

    return key

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
def __validateOrderPreferences(order_preferences):
    error_msgs = []

    # No point in checking further if this check fails:
    if not order_preferences:
        error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', VALIDATION_MSG_ORDER_PREFS))
        raise InputValidationException(error_msgs)

    if(not order_preferences.get('first_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'first_name', VALIDATION_MSG_FIRST_NAME))
    if(not order_preferences.get('last_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'last_name', VALIDATION_MSG_LAST_NAME))

    notification_preferences = order_preferences.get('notification_pref')
    notification_preferences_details = order_preferences.get('notification_pref_details')

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

    if len(error_msgs) > 0:
        raise InputValidationException(error_msgs)

    # Default locale if missing:
    if not order_preferences.get('locale'):
        order_preferences['locale'] = 'en_US'

    # Default is_guest if missing:
    if not order_preferences.get('is_guest'):
        order_preferences['is_guest'] = False
