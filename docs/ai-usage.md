# AI Use Disclosure

AI assistance was used to extract and structure the assignment requirements, compare architecture options, identify database/message-broker and worker-crash failure windows, draft tests and documentation, and review implementation details.

The AI initially recommended using PostgreSQL alone as both durable state and work queue because it was the smallest reliable design for the stated load. The user rejected that recommendation and selected PostgreSQL plus RabbitMQ specifically to demonstrate how a production service handles the common database/message-queue consistency problem. That human decision led to the Transactional Outbox architecture used here.

The design deliberately rejected Kafka, distributed two-phase commit, separate microservice repositories, Kubernetes, dynamic payload templates, online destination administration, and generic exactly-once claims. These choices keep the assignment focused while making delivery guarantees and failure boundaries explicit.

Human decisions included architecture selection, scope and milestone approvals, acceptance of at-least-once delivery and its unavoidable duplicate window, use of pre-registered destinations, and approval of the implementation plan. Generated suggestions were checked against tests, source code, and real PostgreSQL/RabbitMQ integration runs; they were not treated as source truth.

Implementation note: Go 1.27.1 is pinned in CI and the container image. The local development host available during implementation provided Go 1.24.4, so `go.mod` declares Go 1.24 as the minimum language version while avoiding features newer than that minimum.
