CREATE TABLE users (
    id bigserial not null primary key,
    email varchar not null unique,
    encrypted_password varchar not null,
    role_id bigserial not null
);  

CREATE TABLE roles (
    id bigserial not null primary key,
    title varchar not null unique
);

CREATE TABLE repositories (
    id bigserial not null primary key,
    repo varchar not null unique
);

ALTER TABLE users ADD FOREIGN KEY (role_id) REFERENCES roles (id);