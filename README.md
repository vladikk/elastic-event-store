# Elastic Event Store

![Elastic Event Store: AWS Components](./docs/diagrams/aws-components.png)

A serverless implementation of the storage mechanism for event sourcing-based systems.

## What is Event Sourcing?

## What is Event Store?

## Getting Started

1. Install [AWS Serverless Application Model(SAM) CLI] (https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) and configure your [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

2. Clone the repository:

```sh

    $ git clone https://github.com/doitintl/elastic-event-store.git
    $ cd elastic-event-store

```

3. Build and deploy a new instance:

```sh

    $ sam build
    ...
    Build Succeeded

    $ sam deploy --guided
    ...
    Key           ApiEndpoint                                                                        
    Description   API Gateway endpoint URL for Prod stage                                      
    Value         https://XXXXXXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod/
    ------------------------------------------------------------------------------------

```

Pay attention to the values of ReadCapacityUnits and WriteCapacityUnits. Low values limit the event store's, too high values increase the cost.

### Using Elastic Event Store

#### 1. Submit a few changesets


```sh
$ curl https://XXXXXXXX.execute-api.XXXXXXX.amazonaws.com/Prod/streams/stream-aaa-111 \
     --header 'Content-Type: application/json' \
     --request POST \
     --data @- <<BODY
{
    "metadata": {
        "command": "do_something",
        "issuedBy": "me"
    },
    "events": [
        { "type": "init", "data": 1 },
        { "type": "sell", "data": 20 },
        { "type": "buy", "data": 5 }
    ]
}
BODY
```

```sh
$ curl https://XXXXXXXX.execute-api.XXXXXXX.amazonaws.com/Prod/streams/stream-aaa-222 \
     --header 'Content-Type: application/json' \
     --request POST \
     --data @- <<BODY
{
    "metadata": {
        "command": "do_something",
        "issuedBy": "me"
    },
    "events": [
        { "type": "init", "data": 1 },
        { "type": "sell", "data": 20 },
        { "type": "buy", "data": 5 }
    ]
}
BODY
```

```sh
$ curl https://XXXXXXXX.execute-api.XXXXXXX.amazonaws.com/Prod/streams/stream-aaa-111\?expected_last_changeset=1 \
     --header 'Content-Type: application/json' \
     --request POST \
     --data @- <<BODY
{
    "metadata": {
        "command": "do_something_else",
        "issuedBy": "me"
    },
    "events": [
        { "type": "buy", "data": 100 },
        { "type": "buy", "data": 220 },
        { "type": "sell", "data": 15 }
    ]
}
BODY
```

#### 2. Fetch changesets belonging to one of the streams:

```sh
$ curl https://XXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod/streams/stream-aaa-111/changesets\?pp=true

{
    "stream_id": "stream-aaa-111",
    "changesets": [
        {
            "changeset_id": 1,
            "events": [
                {
                    "type": "init",
                    "data": 1
                },
                ...
            ],
            "metadata": {
                "command": "do_something",
                "issuedBy": "me"
            }
        },
        ...
    ]
}       
```

Or you can also fetch the events directly:

```sh
$ curl https://XXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod/streams/stream-aaa-111/events\?pp=true

{
    "stream_id": "stream-aaa-111",
    "events": [
        {
            "id": 1,
            "data": {
                "type": "init",
                "data": 1
            }
        },
        ...
        {
            "id": 6,
            "data": {
                "type": "sell",
                "data": 15
            }
        }
    ]
}   
```

#### 3. Enumerate the changesets in the event store globally (across multiple streams)

```sh
$ curl https://XXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod/changesets\?checkpoint=0\&pp=true

{
    "checkpoint": 0,
    "limit": 10,
    "changesets": [
        ...
        {
            "stream_id": "aaa-111111",
            "changeset_id": 1,
            "events": [],
            "metadata": [],
            "checkpoint": 3
        }
        ....
    ],
    "next_checkpoint": 7
}
```

Notice the "next_checkpoint" value. Use it for getting the next batch of changesets.

## Push Subscriptions

The CloudFormation stack included two SNS topics you can use to get notifications about newly submitted changesets or events:

1. ees_changesets_XXX_XXX_.fifo - for subscribing to new changesets
2. ees_events_XXX_XXX_.fifo - for subscribing to individual events

## Pull/Catchup Subscriptions

## Architecture

## Data Model

## Ordering Guarantees

## Limitations
