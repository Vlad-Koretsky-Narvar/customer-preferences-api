
keyDelim = '|'
NOTIFICATION_PREF_SMS = 'SMS'
NOTIFICATION_PREF_FB = 'FB'
NOTIFICATION_PREF_EMAIL = 'EMAIL'

# COMMON MESSAGES:
VALIDATION_MSG_REQUEST_FAILED_AUTHENTICATION_OR_AUTHORIZATION = 'The client has not provided valid credentials or is not authorized to use this API.'
VALIDATION_MSG_CUSTOMER_ID = 'Invalid input: missing required [customer_id] parameter!'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_LENGTH = 'Invalid input: channels in [notification_pref] must match with their [notification_pref_details].'
VALIDATION_MSG_NOTIFICATION_PREFS_AND_DETAILS_MISMATCH_ITEM = 'Invalid input: missing customer [notification_pref_details] for: '
VALIDATION_MSG_NOTIFICATION_PREFS_CHANNEL = 'Invalid input: invalid notification preference value. Supported values are: ' + ''.join([NOTIFICATION_PREF_SMS, keyDelim, NOTIFICATION_PREF_FB, keyDelim, NOTIFICATION_PREF_EMAIL])
VALIDATION_MSG_ADDRESS_STREET1 = 'Invalid input: missing required parameter [street_1] in address.'
VALIDATION_MSG_ADDRESS_CITY = 'Invalid input: missing required parameter [city] in address.'
VALIDATION_MSG_ADDRESS_STATE = 'Invalid input: missing required parameter [state] in address.'
VALIDATION_MSG_ADDRESS_ZIP = 'Invalid input: missing required parameter [zip] in address.'
VALIDATION_MSG_ADDRESS_COUNTRY = 'Invalid input: missing required parameter [country] in address.'

IN_CUSTOMER_PREFERENCES = ' in customer_preferences!'
IN_ORDER_PREFERENCES = ' in order_preferences!'

# CUSTOMER PREFERENCES MESSAGES:
VALIDATION_MSG_STALE_DATA_MODIFICATION_CP = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to [modified_datetime] field in customer_preferences in the response for correct value to use.'
VALIDATION_MSG_CUST_PREFS = 'Invalid input: missing required [customer_preferences] section parameter in request body.'
VALIDATION_MSG_FIRST_NAME_CP = 'Invalid input: missing required parameter [first_name] in customer_preferences.'
VALIDATION_MSG_LAST_NAME_CP = 'Invalid input: missing required parameter [last_name] in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_CP = 'Invalid input: missing required [notification_pref] section parameter in customer_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS_CP = 'Invalid input: missing required [notification_pref_details] section parameter in customer_preferences.'
VALIDATION_MSG_ADDRESS_CP = 'Invalid input: missing required [address] section parameter in customer_preferences.'
VALIDATION_MSG_POST_DATA_EXISTS_CP = 'Invalid input: you are trying to submit a POST request on an existing customer_preference. Use PUT instead.'
VALIDATION_MSG_PUT_DATA_NOT_EXISTS_CP = 'Invalid input: you are tyring to submit a PUT request on a customer_preference that does not exist. Use POST instead.'

# ORDER PREFERENCES MESSAGES
VALIDATION_MSG_ORDER_ID = 'Invalid input: missing required [order_id] parameter!'
VALIDATION_MSG_STALE_DATA_MODIFICATION_OP = 'Stale data modification: the record you are trying to update has been updated by another process. Please refer to [modified_datetime] field in order_preferences in the response for correct value to use.'
VALIDATION_MSG_ORDER_PREFS = 'Invalid input: missing required [order_preferences] section parameter in request body.'
VALIDATION_MSG_FIRST_NAME_OP = 'Invalid input: missing required parameter [first_name] in order_preferences.'
VALIDATION_MSG_LAST_NAME_OP = 'Invalid input: missing required parameter [last_name] in order_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_OP = 'Invalid input: missing required [notification_pref] section parameter in order_preferences.'
VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS_OP = 'Invalid input: missing required [notification_pref_details] section parameter in order_preferences.'
VALIDATION_MSG_INACTIVE_RECORD_INSERT_OP = 'Invalid input: [is_active] must be TRUE when inserting new order preferences record.'
VALIDATION_MSG_POST_DATA_EXISTS_OP = 'Invalid input: you are trying to submit a POST request on an existing order_preference. Use PUT instead.'
VALIDATION_MSG_PUT_DATA_NOT_EXISTS_OP = 'Invalid input: you are tyring to submit a PUT request on an order_preference that does not exist. Use POST instead.'

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

def validateCustomerPreferences(cust_preferences):
    error_msgs = []

    # No point in checking further if this check fails:
    if not cust_preferences:
        error_msgs.append(ResponseMessage('ERROR', None, 'customer_preferences', VALIDATION_MSG_CUST_PREFS))
        raise InputValidationException(error_msgs)

    if(not cust_preferences.get('first_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'first_name', VALIDATION_MSG_FIRST_NAME_CP))
    if(not cust_preferences.get('last_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'last_name', VALIDATION_MSG_LAST_NAME_CP))

    notification_preferences = cust_preferences.get('notification_pref')
    notification_preferences_details = cust_preferences.get('notification_pref_details')

    # Notification Preference and Notification Preferences Details lists cannot be null (but can be empty)
    if(notification_preferences == None):
        error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref', VALIDATION_MSG_NOTIFICATION_PREFS_CP))
        raise InputValidationException(error_msgs)
    else:
        notification_preferences = sorted(notification_preferences)

    if(notification_preferences_details == None):
        error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref_details', VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS_CP))
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
        error_msgs.append(ResponseMessage('ERROR', None, 'address', VALIDATION_MSG_ADDRESS_CP))
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

def validateOrderPreferences(order_preferences, http_method):
    error_msgs = []

    # No point in checking further if this check fails:
    if not order_preferences:
        error_msgs.append(ResponseMessage('ERROR', None, 'order_preferences', VALIDATION_MSG_ORDER_PREFS))
        raise InputValidationException(error_msgs)

    if(not order_preferences.get('first_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'first_name', VALIDATION_MSG_FIRST_NAME_OP))
    if(not order_preferences.get('last_name')):
        error_msgs.append(ResponseMessage('ERROR', None, 'last_name', VALIDATION_MSG_LAST_NAME_OP))

    notification_preferences = order_preferences.get('notification_pref')
    notification_preferences_details = order_preferences.get('notification_pref_details')

    # Notification Preference and Notification Preferences Details lists cannot be null (but can be empty)
    if(notification_preferences == None):
        error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref', VALIDATION_MSG_NOTIFICATION_PREFS_OP))
        raise InputValidationException(error_msgs)
    else:
        notification_preferences = sorted(notification_preferences)

    if(notification_preferences_details == None):
        error_msgs.append(ResponseMessage('ERROR', None, 'notification_pref_details', VALIDATION_MSG_NOTIFICATION_PREFS_DETAILS_OP))
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

    # Preference must be active in order to be inserted (active => inactive update allowed):
    if(http_method.casefold() == 'post' and order_preferences.get('is_active') == False):
        error_msgs.append(ResponseMessage('ERROR', None, 'is_active', VALIDATION_MSG_INACTIVE_RECORD_INSERT_OP))
        raise InputValidationException(error_msgs)

    # Default locale if missing:
    if not order_preferences.get('locale'):
        order_preferences['locale'] = 'en_US'

    # Default is_guest if missing:
    if not order_preferences.get('is_guest'):
        order_preferences['is_guest'] = False
