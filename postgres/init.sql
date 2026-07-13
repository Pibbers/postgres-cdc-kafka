CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium_password';

GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

CREATE TABLE customer (
    customer_id     BIGINT PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    phone           VARCHAR(30),
    address         VARCHAR(300),
    created_ts      TIMESTAMP NOT NULL DEFAULT now(),
    updated_ts      TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE customer REPLICA IDENTITY FULL;

CREATE TABLE account (
    account_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    account_type    VARCHAR(20),
    status          VARCHAR(20),
    opened_ts       TIMESTAMP NOT NULL DEFAULT now(),
    updated_ts      TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE account REPLICA IDENTITY FULL;

CREATE TABLE card (
    card_id             BIGINT PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES account(account_id),
    card_number_masked  VARCHAR(20),
    card_type           VARCHAR(20),
    status              VARCHAR(20),
    issued_ts           TIMESTAMP NOT NULL DEFAULT now(),
    updated_ts          TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE card REPLICA IDENTITY FULL;

CREATE TABLE payment (
    payment_id      BIGINT PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES account(account_id),
    amount          DECIMAL(12,2),
    currency        CHAR(3) DEFAULT 'USD',
    payment_method  VARCHAR(20),
    status          VARCHAR(20),
    payment_ts      TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE payment REPLICA IDENTITY FULL;

CREATE TABLE transaction (
    transaction_id  BIGINT PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES account(account_id),
    card_id         BIGINT REFERENCES card(card_id),
    amount          DECIMAL(12,2),
    currency        CHAR(3) DEFAULT 'USD',
    merchant        VARCHAR(100),
    status          VARCHAR(20),
    transaction_ts  TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE transaction REPLICA IDENTITY FULL;

CREATE PUBLICATION debezium_publication FOR TABLE
    public.customer, public.account, public.card, public.payment, public.transaction;
