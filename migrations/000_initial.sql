-- noinspection SqlNoDataSourceInspectionForFile

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS locked_object_store AUTHORIZATION CURRENT_USER;

CREATE TABLE IF NOT EXISTS locked_object_store.locks(
    record_version_number UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    owner_name VARCHAR(255) DEFAULT NULL,
    lease_duration BIGINT DEFAULT NULL,
    is_released BOOLEAN DEFAULT false,
    is_non_acquirable BOOLEAN DEFAULT false,
    data JSONB DEFAULT NULL,
    lookup_time TIMESTAMP DEFAULT NOW()
);