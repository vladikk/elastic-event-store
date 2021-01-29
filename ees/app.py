import json
from ees.commands.version import Version
from ees.commands.commit import Commit
from ees.commands.invalid import InvalidEndpoint

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    endpoint = route_request(event, context)
    return endpoint.execute(event, context)


def route_request(event, context):
    handlers = {
        "/version": Version(),
        "/version/": Version(),
        "/commit": Commit(),
        "/commit/": Commit()
    }
    
    path = event["path"].lower()
    print(f'Requested path: {path}')

    if path in handlers.keys():
        return handlers[path]
    else:
        return InvalidEndpoint()