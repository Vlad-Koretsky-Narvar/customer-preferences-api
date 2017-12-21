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
EXCEPTION_SAVING_CUSTOMER_PREFERENCE = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to modified_datetime field in customer_details in the response for correct value to use.'
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

class Error(Exception):
    '''Base class for server validation exceptions'''
    responseCode = 400

class InputValidationException(Exception):
    '''Server Validation Exception'''
    responseCode = 400
    messages = []
    def __init__(self, err_msgs):
        Exception.__init__(self)
        self.messages = err_msgs
'''
class InputValidationRetailerMonikerException(Error):
    # Server Validation Exception
    msg = 'Missing parameter [retailer_moniker] in query parameters in the request!'

class InputValidationMissingCustomerPreferencesException(Error):
    # Server Validation Exception
    msg = 'Missing parameter [customer_preferences] in request body!'

class StaleDataModificationException(Error):
    # Server Validation Exception
    msg = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to modified_datetime field in customer_details in the response for correct value to use.'
'''

def method_get(event, context):
    '''
    retailer_moniker = 'narvar-speede'
    customer_id = '123'
    customerKey = makeKey(retailer_moniker, customer_id)
    '''
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
            raise InputValidationException(error_msgs) #InputValidationRetailerMonikerException
    except InputValidationException as ivrme: #InputValidationRetailerMonikerException as ivrme:
        exception = ivrme

    customer_details = {}
    if exception != None:
        return makeResponse(customer_details, exception, None)
    # END OF: Basic input validation

    customerKey = makeKey(retailer_moniker, customer_id)

    result = {}
    try:
        result = findCustomerPreference(customerKey)
        customer_details = makeCustomerDetails(result)
    except Exception as e:
        exception = e # Preserve exception for the response.
        # TODO: Log the exception.

    return makeResponse(customer_details, exception, None)

def method_post2(event, context):
    exception = None
    customer_details = {}

    try:
        if not event or not event.get('body') or not event.get('body'):
            raise InputValidationMissingCustomerPreferencesException
        req_body = json.loads(event.get('body'))
        chupa = req_body.get('chupa')
        if not chupa:
            raise InputValidationMissingCustomerPreferencesException
    except InputValidationMissingCustomerPreferencesException as ivmcpe:
        exception = ivmcpe

    if exception != None:
        return makeResponse(customer_details, exception, event)
    else:
        return makeResponse(customer_details, exception, event)

def validateCustomerPreferencesDict(cust_preferences_dict):
    if not cust_preferences_dict:
        raise Exception('Invalid input: customer preferences cannot be null.')
    if(not cust_preferences_dict.get('fname')):
        raise Exception('Invalid input: customer first name cannot be null.')
    if(not cust_preferences_dict.get('lname')):
        raise Exception('Invalid input: customer last name cannot be null.')
    if(not cust_preferences_dict.get('locale')):
        raise Exception('Invalid input: customer locale cannot be null.')

    notification_preferences = cust_preferences_dict.get('notification_pref')
    notification_preferences_details = cust_preferences_dict.get('notification_pref_details')

    # Notification Preference and Notification Preferences Details lists cannot be null (but can be empty)
    if(notification_preferences == None):
        raise Exception('Invalid input: customer notification preferences cannot be null.')
    else:
        notification_preferences = sorted(notification_preferences)

    if(notification_preferences_details == None):
        raise Exception('Invalid input: customer notification preferences details cannot be null.')
    notification_prefs_details_sorted = sorted(notification_preferences_details, key=lambda k: k['name'])

    # Notification Preferences and Notification Preferences Details must be of the same length:
    if len(notification_preferences) != len(notification_preferences_details):
        raise Exception('Invalid input: customer notification preferences channels must match their details.')

    # Loop through the channels and verify that the corresponding item is present in details:
    idx = 0
    for item in notification_preferences:
        # Check for blank values:
        if not item:
            raise Exception('Invalid input: notification preference channels cannot be empty/null.')
        # Check for allowed value options:
        if not (item == NOTIFICATION_PREF_SMS or item == NOTIFICATION_PREF_FB or item == NOTIFICATION_PREF_EMAIL):
            raise Exception('Invalid input: invalid notification preference value. Supported values are: ' +
                            ''.join(NOTIFICATION_PREF_SMS, keyDelim, NOTIFICATION_PREF_FB, keyDelim, NOTIFICATION_PREF_EMAIL))
        # Make sure that there is a corresponding option for Notification Preferences Details:
        details = notification_prefs_details_sorted[idx]
        if not details or item != details.get('name') or not details.get('value'):
            raise Exception('Invalid input: missing customer notification preferences details for: ' + item)

        idx += 1

    # Validate address fields:
    address = cust_preferences_dict.get('address')
    if(not address):
        raise Exception('Invalid input: customer address cannot be null.')
    if not address.get('street_1'):
        raise Exception('Invalid input: address street_1 field value cannot be null/empty.')
    if not address.get('city'):
        raise Exception('Invalid input: address city field value cannot be null/empty.')
    if not address.get('state'):
        raise Exception('Invalid input: address state field value cannot be null/empty.')
    if not address.get('zip'):
        raise Exception('Invalid input: address zip code field value cannot be null/empty.')
    if not address.get('country'):
        raise Exception('Invalid input: address country field value cannot be null/empty.')


def method_post(event, context):
    retailer_moniker = 'narvar-speede'
    customer_id = '123'

    address_dict = {}
    address_dict['street_1'] = "123 Somewhere Street"
    address_dict['street_2'] = "apt # 6"
    address_dict['city'] = "SomeTown"
    address_dict['state'] = "CA"
    address_dict['country'] = "USA"
    address_dict['zip'] = "12345"

    notification_pref = ['EMAIL', 'SMS']
    notification_pref_details = [{'name': 'SMS', 'value': '999-999-9999'}, {'name': 'EMAIL', 'value': 'chupa@cabra.com'}]

    json_dict = {}
    json_dict['fname'] = "Vlad"
    json_dict['lname'] = "Test"
    json_dict['locale'] = "en_US"
    json_dict['address'] = address_dict
    json_dict['notification_pref'] = notification_pref
    json_dict['notification_pref_details'] = notification_pref_details

    created_datetime = "2017-11-14T01:16:36.565029"
    modified_datetime = "2017-12-20T22:53:30.122659"

    exception = None
    try:
        saveCustomerPreference(retailer_moniker, customer_id, json_dict, created_datetime, modified_datetime)
    except StaleDataModificationException as sdme:
        exception = sdme
    except Exception as e:
        exception = e # Preserve exception for later response.
        # TODO: Log exception here.

    customerKey = makeKey(retailer_moniker, customer_id)
    search_result = findCustomerPreference(customerKey)
    customer_details = makeCustomerDetails(search_result)

    statusCode = 200
    body = {}
    if not exception:
        # Success:
        body['message'] = 'Customer preferences successfully saved.'
    else:
        # Failure:
        body['message'] = 'Exception saving customer preferences!'
        if type(exception) == StaleDataModificationException:
            statusCode = exception.responseCode
            body['exception_message'] = exception.msg
        else:
            statusCode = 500
            body['exception_message'] = exception.args[0]

    body['input'] = event
    body['customer_details'] = customer_details

    response = {
        "statusCode": statusCode,
        "body": json.dumps(body)
    }
    return response

def saveCustomerPreference(retailer_moniker, customer_id, cust_preferences_dict, created_datetime, modified_datetime):
    if not retailer_moniker:
        raise Exception('Invalid input: retailer_moniker cannot be empty/null.')
    if not customer_id:
        raise Exception('Invalid input: customer_id cannot be empty/null.')
    #validateCustomerPreferencesDict(cust_preferences_dict)

    db = boto3.client('dynamodb');

    id = makeKey(retailer_moniker, customer_id)

    if not modified_datetime:
        modified_datetime = datetime.datetime.utcnow().isoformat()
    if not created_datetime:
        created_datetime = modified_datetime

    # Find if the record already exists and perform some checks:
    dbVersion = findCustomerPreference(id)
    if(dbVersion):
        # Overwrite retailer_moniker, customer_id and created_datetime values in the request (modification of these is not allowed):
        retailer_moniker = dbVersion.get('retailer_moniker')
        customer_id = dbVersion.get('customer_id')
        created_datetime = dbVersion.get('created_datetime')

        if dbVersion.get('modified_datetime') != modified_datetime:
            #raise Exception(EXCEPTION_SAVING_CUSTOMER_PREFERENCE)
            raise StaleDataModificationException()
        else:
            modified_datetime = datetime.datetime.utcnow().isoformat()

    # Make a string version of JSON to store:
    customer_pref_json = json.dumps(cust_preferences_dict)

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
    print("Response for inserting a record for the key: ", id, str(response))

    return True

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

def makeKey(retailer_moniker, customer_id):
    return ''.join([retailer_moniker, keyDelim, customer_id])

def makeCustomerDetails(response):
    result = {}

    if response == None:
        return result

    if response.get('customer_pref_json') != None:
        customer_pref_json = json.loads(response.get('customer_pref_json'))

        result['fname'] = customer_pref_json.get('fname')
        result['lname'] = customer_pref_json.get('lname')
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

def makeResponse(customer_details, exception, event):
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

    body['customer_details'] = customer_details
    if(event):
        body['input'] = event

    response = {
        "statusCode": statusCode,
        "body": json.dumps(body)
    }
    return response
