# Hulygun 🔫
Hulygun is a worker that routes Huly communication event requests from Apache Kafka topic(s) to Huly transactors.

## Operation
Hulygun consumes messages from specified topics. It inspects the message for __AccountUid__ header and uses it's value to determine the transactor to which the request should be routed. If the transactor is not found in the cache, it will be looked up in the Huly accounts service. The transactor is then cached for future use (forever).

Complete message body is send to the transactor's ```/api/v1/event/{WorkspaceUid}``` endpoint. If the HTTP request fails and the error is classfied as transient (5xx), several more retries are made with exponential backoff. At this point, backoff parameters are hardcoded and are not configurable.

Hulygun authenticates requests with JWT. Token claims are built ofh the system account id, workspace id and service name. Huly secret is used to sign JWT's.

Hulygun employs a limiter to control the rate at which transactor requests are made. The limiter is global (the same for all transactors) and limits the number of requests per second. 

## Configuration
The following environment variables are used to configure Hulygun:

- ```HULY_GROUP_ID``` - Kafka consumer group id. Default: hulygun
- ```HULY_TOPICS``` - Comma separated list of Kafka topics to consume from. Default: ["hulygun"]
- ```HULY_KAFKA_BOOTSTRAP``` - Comma separated list of bootstrap Kafka brokers. Default: localhost:19092
- ```HULY_SECRET``` - Huly secret used to sign requests to account service and transactors. Default: secret
- ```HULY_ACCOUNTS_SERVICE``` - URL of the Huly accounts service. Default: http://localhost:8080/account
- ```HULY_SERVICE_ID``` - Huly service id. Default: hulygun
- ```HULY_RATE_LIMIT``` - Maximal request rate per transactor. Default: 10 (requests per second)
- ```HULY_DRY_RUN``` - Dry run mode. If set to true, no requests are sent to transactors and consumed messages are not commited. Default: false

## IMPORTANT
It is __strongly__ recommened to use ```CardID``` as a message key. This will ensure that all messages for the card are processed in the correct order. 

## Further steps
- [ ] Limit requests per transactor (currently limiter is global)
- [ ] More flexibale error classification 
- [ ] Support for temprary unavailable workspaces (i.e. in migration state)

## License
This project is licensed under EPL-2.0
