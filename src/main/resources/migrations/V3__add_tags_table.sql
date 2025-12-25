create table metrics._tag
(
    id   serial primary key,
    name text unique not null
);