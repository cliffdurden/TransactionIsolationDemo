# Demo shows how @Transactional(isolation = ...) annotation affects on the result

PostgreSQL 15 is used in this demo.

Tests that show the different outcomes are in `TransactionIsolationDemoApplicationTests.java`

according to [PostgreSQL doc](https://www.postgresql.org/docs/current/transaction-iso.html)

| **Isolation Level**  | **Dirty Read**         | **Nonrepeatable Read** | **Phantom Read**       | **Serialization Anomaly** |
|----------------------|------------------------|------------------------|------------------------|---------------------------|
| **Read uncommitted** | Allowed, but not in PG | Possible               | Possible               | Possible                  |
| **Read committed**   | Not possible           | Possible               | Possible               | Possible                  |
| **Repeatable read**  | Not possible           | Not possible           | Allowed, but not in PG | Possible                  |
| **Serializable**     | Not possible           | Not possible           | Not possible           | Not possible              |