# Elastic Event Store

![Elastic Event Store: AWS Components](./docs/diagrams/aws-components.png)

A serverless implementation of the storage mechanism for event sourcing-based systems.

## What is Event Sourcing?

Traditionally, software systems operate on state-based data. In other words, business entities and concepts are represented as a snapshot of their *current* state. E.g.:

| Id  | Name       | Team              |
| --- | ---------- | ----------------- |
| 1   | Gillian    | Administration    |
| 2   | Krzysztof  | Accounting        |
| 3   | Robyn      | Frontend          |

In the above example, all we know about the data is its current state. *But how did it get to the current state?* - We don't know. All we know is entity's current state. The Event Sourcing pattern does answer this, and many other questions.

The Event Sourcing pattern introduces the dimension of time into the modeling of business entities and their lifecycles. Instead of capturing an entity's current state, an event sourced system keep a transactional record of all events that have occured during an entity's lifecycle. For example:

```
{ "id": 3, "type": "initialized", "name": "Robyn", "timestamp": "2021-01-05T13:15:30Z" }
{ "id": 3, "type": "assigned", "team": "Frontend", "timestamp": "2021-01-05T16:15:30Z" }
{ "id": 3, "type": "promoted", "position": "team-leader", "timestamp": "2021-01-22T16:15:30Z" }

```

Modeling and persisting the events captures what exactly happenned during an entity's lifecycle. The events are used as the system's **source of truth**. Hence the name of the pattern -- event sourcing.

Not only we can derive the current state by sequentially applying the events, but the flexbile events-based model allows projecting different state models, optimized for different tasks.

Finally, Event Dourcing is **not** event driven architecture:

> "EventSourcing is not Event driven architecture."
>
> The former is about events _inside_ the app.
>
> The latter is about events _between_ (sub)systems
>
> ~ [@ylorph](https://twitter.com/ylorph/status/1295480789765955586)

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

## Push Subscriptions

The CloudFormation stack included two SNS topics you can use to get notifications about newly submitted changesets or events:

1. ees_changesets_XXX_XXX_.fifo - for subscribing to new changesets
2. ees_events_XXX_XXX_.fifo - for subscribing to individual events

## Pull(Catchup) Subscriptions

You can enumerate the changesets globally (across multiple streams) using the "changesets" endpoint:

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
            "events": [
                ...
            ],
            "metadata": [
                ...
            ],
            "checkpoint": 3
        }
        ....
    ],
    "next_checkpoint": 7
}
```

Notice the "next_checkpoint" value. Use it for getting the next batch of changesets.

## Architecture

## Data Model

## Ordering Guarantees

1. The order of changesets and events in a stream is preserved and is strongly consistent.

2. The order of changesets across all the streams is not guaranteed to be exactly the same as the order in which the streams were updated. That said, the inter-stream order is repeatable. I.e., when calling the global changesets endpoint ("/changesets"), the changesets are always returned in the same order.

## Limitations

Since DynomoDB is used as the storage mechanism, its limitations apply to Elastic Event Store:

1. The maximum item size in DynamoDB, and hence the maximum changeset size, is 400 KB.
2. The maximum size of a DynamoDB item collection is 10GB, hence this is the maximum size of a single stream.

Finally, as with all serverless solutions, beyond a certain scale it can be more cost effective to use a self managed solution.
