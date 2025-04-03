# Elastic Event Store

A fully serverless storage for event sourcing-based systems.

![Elastic Event Store: AWS Components](./docs/diagrams/aws-components.png)

## Table of Contents

- [What is Event Sourcing?](#WhatIsEventSourcing)
- [What is Event Store?](#WhatIsEventStore)
- [Getting Started](#GettingStarted)
  * [Installing](#Installing)
  * [Using](#Using)
- [Push Subscriptions](#PushSubscriptions)
- [Pull Subscriptions](#PullSubscriptions)
- [Architecture](#Architecture)
- [Data Model](#DataModel)
- [Ordering Guarantees](#OrderingGuarantees)
- [Testing](#Testing)
- [Limitations](#Limitations)

<a name="WhatIsEventSourcing"/>

## What is Event Sourcing?

Traditionally, software systems operate on state-based data. In other words, business entities and concepts are represented as a snapshot of their *current* state. E.g.:

| Id  | Name       | Team              |
| --- | ---------- | ----------------- |
| 1   | Gillian    | Administration    |
| 2   | Krzysztof  | Accounting        |
| 3   | Robyn      | Frontend          |

In the above example, all we know about the data is its current state. *But how did it get to the current state?* — We don't know. The Event Sourcing pattern answers this and many other questions.

Event Sourcing introduces the dimension of time into the modeling of business entities and their lifecycles. Instead of capturing an entity's current state, an event-sourced system keeps a transactional record of all events that have occurred during an entity's lifecycle. For example:

```
{ "id": 3, "type": "initialized", "name": "Robyn", "timestamp": "2021-01-05T13:15:30Z" }
{ "id": 3, "type": "assigned", "team": "Frontend", "timestamp": "2021-01-05T16:15:30Z" }
{ "id": 3, "type": "promoted", "position": "team-leader", "timestamp": "2021-01-22T16:15:30Z" }
```

By modeling and persisting events, we capture exactly what happened during an entity's lifecycle. Events become the system's **source of truth**. Hence the name: event sourcing.

Not only can we derive the current state by sequentially applying events, but the flexible event-based model also allows projecting different state models optimized for different tasks.

Finally, Event Sourcing is **not** Event-Driven Architecture (EDA):

> EventSourcing is not Event driven architecture. The former is about events _inside_ the app. The latter is about events _between_ (sub)systems  
> ~ [@ylorph](https://twitter.com/ylorph/status/1295480789765955586)

<a name="WhatIsEventStore"/>

## What is Event Store?

An event store is a storage mechanism optimized for event-sourcing-based systems. It should provide the following functionality:

1. Append events to a stream (stream = events of a distinct entity).
2. Read events from a stream.
3. Concurrency management to detect collisions when multiple processes write to the same stream.
4. Enumerate events across all streams (e.g., for CQRS projections).
5. Push newly committed events to interested subscribers.

All of the above functions are supported by the Elastic Event Store.

<a name="GettingStarted"/>

## Getting Started

<a name="Installing"/>

### Installing

1. Install [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) and configure your [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

2. Clone the repository:

```sh
git clone https://github.com/doitintl/elastic-event-store.git
cd elastic-event-store
```

3. Build and deploy a new instance:

```sh
sam build
# ... Build Succeeded

sam deploy --guided
# ...
# ApiEndpoint: https://XXXXXXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod/
```

Verify installation:

```sh
curl https://XXXXXXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod/version
# { "version": "0.0.1" }
```

<a name="Using"/>

### Using

#### 1. Submit a few changesets

```sh
EES_URL=https://XXXXXXXXXXXX.execute-api.XXXXXXXX.amazonaws.com/Prod
```

```sh
curl $EES_URL/streams/stream-aaa-111 \
     -H 'Content-Type: application/json' \
     -X POST \
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

The Elastic Event Store is opinionated about concurrency control: it is mandatory. When committing to an existing stream, you must specify the expected last changeset:

```sh
curl "$EES_URL/streams/stream-aaa-111?expected_last_changeset=1" \
     -H 'Content-Type: application/json' \
     -X POST \
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

#### 2. Fetch changesets:

```sh
curl $EES_URL/streams/stream-aaa-111/changesets
```

#### 3. Fetch events:

```sh
curl $EES_URL/streams/stream-aaa-111/events
```

#### 4. Fetch statistics:

```sh
curl $EES_URL/streams
```

> Note: Statistics are updated asynchronously every minute.

<a name="PushSubscriptions"/>

## Push Subscriptions

The CloudFormation stack includes two SNS FIFO topics:

1. `ees_changesets_XXX_XXX_.fifo` — for new changesets
2. `ees_events_XXX_XXX_.fifo` — for individual events

<a name="PullSubscriptions"/>

## Pull (Catchup) Subscriptions

To enumerate global changesets:

```sh
curl "$EES_URL/changesets?checkpoint=0"
```

Use the `next_checkpoint` value to fetch the next batch. This endpoint is critical for CQRS projections and state rebuilds.

<a name="Architecture"/>

## Architecture

![Elastic Event Store: AWS Components](./docs/diagrams/aws-components.png)

- REST API exposed via API Gateway
- System logic in AWS Lambda
- Events stored in DynamoDB
- DynamoDB Streams trigger Lambdas for global indexing and publishing
- SNS FIFO topics for push subscriptions
- SQS DLQs for failed stream processing

<a name="DataModel"/>

## Data Model

Each partition in the events table represents a stream — i.e., a business entity's event history.

Main DynamoDB schema:

| Column            | Type                  | Description |
| ----------------- | --------------------- | ----------- |
| stream_id         | Partition Key (String) | Stream ID |
| changeset_id      | Sort Key (Number)     | Commit ID in stream |
| events            | JSON (String)         | Committed events |
| metadata          | JSON (String)         | Changeset metadata |
| timestamp         | String                | Commit timestamp |
| first_event_id    | LSI (Number)          | First event ID in stream |
| last_event_id     | LSI (Number)          | Last event ID in stream |
| page              | GSI Partition (Number) | For global ordering |
| page_item         | GSI Sort (Number)     | Index within global page |

<a name="OrderingGuarantees"/>

## Ordering Guarantees

1. **Intra-stream order** is preserved and strongly consistent.
2. **Inter-stream order** is not guaranteed but is repeatable — global enumeration always yields the same result.

<a name="Testing"/>

## Testing

1. Set the `SAM_ARTIFACTS_BUCKET` environment variable:

```sh
export SAM_ARTIFACTS_BUCKET=your-bucket-name
```

2. Deploy the test environment:

```sh
./deploy-test-env.sh
```

3. Run unit tests:

```sh
./run-unit-tests.sh
```

4. Run unit and integration tests:

```sh
./run-all-tests.sh
```

<a name="Limitations"/>

## Limitations

Because DynamoDB is used:

1. Maximum item (changeset) size: 400 KB
2. Maximum item collection (stream) size: 10 GB

As with all serverless solutions, at high scale, a self-managed deployment may be more cost-effective.
