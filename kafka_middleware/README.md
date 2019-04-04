# Kafka Twitter

## Assignment

Implement a simplified version of the Twitter social network using Kafka to store tweets. Users interact with Twitter using a client that presents a timeline of tweets and allows users to publish new tweets.

Tweets have the following structure (Avro serialization is recommended but not mandatory):
Tweet <Authors, Content, Timestamp, Location, [Tags], [Mentions]>

Clients can be configured to filter the tweets that appear in the timeline by tag, location, or mentions.

Clients can update their timeline either in batch or in streaming (continuous) mode. In batch mode, the timeline is updated upon user request, while in streaming mode it is constantly updated to reflect the tweets produced in the last 5 minutes.

Timelines are ordered from the least to the most recent tweet, according to their timestamp (event time).

The communication with the client must be RESTful, with tweets represented in JSON format.

REST API:

- Subscribe to twitter - POST /users/id
- Write a new tweet - POST /tweets JSON Body
- Read tweets (polling) - GET /tweets/{filter}/latest
- Server-Sent Event (streaming) - POST /tweets/{filter} JSONBody [List of tweets]
Requirements:

Exactly once semantics: a tweet is always stored and presented in timelines exactly once.
Upon restart/failure clients should not read all the posts from scratch.
Assumptions:

Clients’ disks are reliable.
Kafka Topics with replication factor > 1 are reliable.

## Technologies

- **Kafka** - a distributed streaming platform that is used for building real-time data pipelines and streaming apps. It is horizontally scalable, fault-tolerant, wicked fast, and runs in production in thousands of companies.
- **Spark REST** - a micro framework for creating web applications in Java 8 with minimal effort.
- **Websocket** - a computer communications protocol, providing full-duplex communication channels over a single TCP connection.

## REST APIs

- **POST /users/{id}** creates a user. Returns a success message if the user has been correctly created, otherwise it returns an error message.
- **POST /tweets** creates a tweet. The tweet must be in the body of the message and it must be in a JSON format. It returns the tweet that has been created.
- **GET /tweets/{filter}/latest** retrieve tweets using a filter set. The filter is encoded as /{location}/{tag}/{mention}.
- **POST /tweets/subscription/{filter}** creates a subscription. The server will notify the client whenever it receives a tweet that matches the filter.

#### Filtering strategy
**Single parameter filter**
- {*Milan/all/all*} : returns all tweet which location is 'Milan'.
- {*all/sunny_day/all*} : returns all tweet which tags contain 'sunny_day'.
- {*all/all/Mario_Rossi*} : returns all tweet which mentions contain 'Mario_Rossi'.

**Multiple parameter filter**
- {*Milan/sunny_day/all*} : returns all tweet which location is 'Milan'and tags contain 'sunny_day'.
- {*Milan/sunny_day/Mario_Rossi*} : returns all tweet which location is 'Milan', tags contain 'sunny_day' and mentions contain 'Mario_Rossi'.

**Single parameter composed filter**
- {*Milan&Rome/all/all*} : returns all tweet which location is 'Milan' or 'Rome'.

**Multiple parameter composed filter** 
- {*Milan&Rome/sunny_day&cloudy_day/Mario_Rossi*} : returns all tweet which location is 'Milan' or 'Rome', tags contain 'sunny_day' or 'cloudy_day', and mentions contain 'Mario_Rossi'.

## Exactly Once Semantic
The application implements the EOS (Exactly Once Semantic). The application is using producer transactions to atomically write in Kafka so on the Consumer side, you have two options for reading transactional messages, expressed through the “isolation.level” consumer config ('read_committed' and 'read_uncommitted').


## Authors

* **Luca Ferrera** (https://github.com/BananaGod)
* **Davide Yi Xian Hu** (https://github.com/DragonBanana)
* **Luca Terracciano** (https://github.com/SwagBanana)