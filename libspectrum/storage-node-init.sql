CREATE DATABASE IF NOT EXISTS spectrum;
USE spectrum;

CREATE TABLE IF NOT EXISTS events (
    xid BIGINT,
    event_id BIGINT,
    type INT,
    body TEXT,
    PRIMARY KEY (xid, event_id)
);

CREATE TABLE IF NOT EXISTS commits (
    commit_id BIGINT PRIMARY KEY,
    xid BIGINT
);