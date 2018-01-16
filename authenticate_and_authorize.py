from __future__ import print_function
from http import HTTPStatus

import json
import re
import requests

API_NAME = 'customer-preferences'
BEARER_CERTIFICATE = '54d7af99-60e5-474c-ab06-fe273077d3c3' # Hard-coded for now here and in AuthenticateAndAuthorizeService.

# TODO: This needs to be environment-based!
BASE_AUTH_URL = 'https://blojewhreg.localtunnel.me/api/authenticate-authorize'

# This method will call an internal service (NarvarApps) that will perform authentication and authorization.
# If successful, authorization header credentials will be translated into a retailer_moniker that is needed for
# further processing.
def authenticateAndAuthorizeRequest(event):
    result = {}
    result['retailer_moniker'] = None

    authorization_header = event.get('headers').get('Authorization')
    #print("authorization_header = " + authorization_header)
    if not authorization_header:
        result['status'] = HTTPStatus.UNAUTHORIZED.value
        return result

    basic_auth_header_regex = re.compile(r'^(?:basic)(.+$)', re.IGNORECASE)
    mo = basic_auth_header_regex.search(authorization_header)

    print("VLAD: match object not empty is: " + str(mo))
    print("VLAD: number of groups is: " + str(0 if not mo else mo.groups().__len__()))
    if not mo or mo.groups().__len__() != 1:
        print("Authorization is missing or is of the wrong type: " + authorization_header)
        result['status'] = HTTPStatus.UNAUTHORIZED.value
        return result

    # Construct URL:
    request_params = { 'apiName': API_NAME }
    print("VLAD: request_params are: " + json.dumps(request_params))

    # Populate headers:
    request_headers = {}
    request_headers.update({'Authorization': authorization_header})
    request_headers.update({'Bearer': BEARER_CERTIFICATE})
    request_headers.update({'Content-Type': 'application/json'})
    print("VLAD: request_headers are: " + json.dumps(request_headers))
    print("VLAD: url is: " + BASE_AUTH_URL)

    # Send request:
    response = requests.get(BASE_AUTH_URL, params = request_params, headers = request_headers)
    if not response:
        print("Something went wrong: no response from AuthenticateAndAuthorize service call... response is empty")
        result['status'] = HTTPStatus.UNAUTHORIZED.value
        return result

    print("VLAD: response.status_code = " + str(response.status_code))

    if response.status_code != HTTPStatus.OK.value:
        print("Something went wrong: no response from AuthenticateAndAuthorize service call... response is: " + str(response.status_code))
        result['status'] = response.status_code
        return result

    payload_response = response.json()
    result['status'] = HTTPStatus.OK.value
    result['retailer_moniker'] = payload_response.get('retailerMoniker')

    return result