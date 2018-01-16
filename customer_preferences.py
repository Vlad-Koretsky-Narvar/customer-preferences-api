from __future__ import print_function
from http import HTTPStatus
from request_validator import ResponseMessage, InputValidationException

import authenticate_and_authorize
import boto3
import datetime
import json
import request_validator
import traceback

API_NAME = 'customer-preferences'
CUST_TABLE_NAME = 'customer-preferences-dev'
keyDelim = '|'

def method_get(event, context):
    exception = None

    # Basic input validation:
    try:
        error_msgs = []

        auth_response = authenticate_and_authorize.authenticateAndAuthorizeRequest(event)
        retailer_moniker = auth_response.get('retailer_moniker')
        #print("auth_response is: " + json.dumps(auth_response))
        if auth_response.get('status') != HTTPStatus.OK.value or not retailer_moniker:
            error_msgs.append(ResponseMessage('ERROR', None, None, request_validator.VALIDATION_MSG_REQUEST_FAILED_AUTHENTICATION_OR_AUTHORIZATION))

        customer_id = event.get('pathParameters').get('customer_id')
        if not customer_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_id', request_validator.VALIDATION_MSG_CUSTOMER_ID))
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
        customer_id = event.get('pathParameters').get('customer_id')
        if not customer_id:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_id', request_validator.VALIDATION_MSG_CUSTOMER_ID))

        auth_response = authenticate_and_authorize.authenticateAndAuthorizeRequest(event)
        retailer_moniker = auth_response.get('retailer_moniker')
        #print("auth_response is: " + json.dumps(auth_response))
        if auth_response.get('status') != HTTPStatus.OK.value or not retailer_moniker:
            error_msgs.append(ResponseMessage('ERROR', None, None, request_validator.VALIDATION_MSG_REQUEST_FAILED_AUTHENTICATION_OR_AUTHORIZATION))

        if len(error_msgs) > 0:
            raise InputValidationException(error_msgs)

        if not event or not event.get('body'):
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', request_validator.VALIDATION_MSG_CUST_PREFS))
            raise InputValidationException(error_msgs)

        cust_prefs = json.loads(event.get('body')).get('customer_preferences')
        if not cust_prefs:
            error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', request_validator.VALIDATION_MSG_CUST_PREFS))
            raise InputValidationException(error_msgs)

        request_validator.validateCustomerPreferences(cust_prefs)
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
        error_msgs.append(ResponseMessage('ERROR', None, None, request_validator.VALIDATION_MSG_POST_DATA_EXISTS_CP))
        raise InputValidationException(error_msgs)
    elif(http_method.casefold() == 'put' and not dbRec):
        error_msgs.append(ResponseMessage('ERROR', None, None, request_validator.VALIDATION_MSG_PUT_DATA_NOT_EXISTS_CP))
        raise InputValidationException(error_msgs)

    if(dbRec):
        # Overwrite retailer_moniker, customer_id and created_datetime values in the request (modification of these is not allowed):
        retailer_moniker = dbRec.get('retailer_moniker')
        customer_id = dbRec.get('customer_id')
        created_datetime = dbRec.get('created_datetime')

        if dbRec.get('modified_datetime') != modified_datetime:
            error_msgs = []
            error_msgs.append(ResponseMessage('ERROR', None, 'modified_datetime', request_validator.VALIDATION_MSG_STALE_DATA_MODIFICATION_CP))
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
        TableName = CUST_TABLE_NAME,
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
        TableName=CUST_TABLE_NAME,
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
