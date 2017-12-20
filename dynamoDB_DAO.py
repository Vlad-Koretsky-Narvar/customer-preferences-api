import boto3
import datetime
import json
#from operator import itemgetter
import traceback

cust_table_name = 'customer-preferences-dev'
#order_table_name = 'order_preferences'
keyDelim = '|'
#notification_preference_sms = 'SMS'
#notification_preference_fb = 'FB'
#notification_preference_email = 'EMAIL'

def method_get(event, context):
    retailer_moniker = 'narvar-speede'
    customer_id = '123'
    customerKey = makeKey(retailer_moniker, customer_id)
    result = findCustomerPreference(customerKey)

    customer_details = makeCustomerDetails(result)

    body = {
        "message": "get() method executed successfully!",
        "input": event,
        "customer_details": customer_details,
	    #"customerKey": customerKey,
        #"is_found": is_found,
        #"does_table_exist": does_table_exist,
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

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
    modified_datetime = "2017-12-20T20:17:59.675110"

    is_before_save = True
    exception = ''
    try:
        saveCustomerPreference(retailer_moniker, customer_id, json_dict, created_datetime, modified_datetime)
    except Exception as e:
        exception = traceback.format_exc()
        body = {
            "message": "post() method executed successfully!",
            "input": event,
       	    "customer_details": customer_details,
            #"customerKey": customerKey,
            "exception": traceback.format_exc(),
            "is_before_save": is_before_save
        }
        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }
        return response

    customerKey = makeKey(retailer_moniker, customer_id)
    search_result = findCustomerPreference(customerKey)
    customer_details = makeCustomerDetails(search_result)

    body = {
        "message": "post() method executed successfully!",
        "input": event,
   	    "customer_details": customer_details,
        #"customerKey": customerKey,
        "exception": exception,
        "is_before_save": is_before_save
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response

def makeKey(retailer_moniker, customer_id):
    return ''.join([retailer_moniker, keyDelim, customer_id])

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
            raise Exception('Stale data modification: the record you are trying to update has been updated by another process.')
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
