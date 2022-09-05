create table items
(
    id       bigserial primary key,
    created  timestamp(3) default now(),
    curve_id bigint  not null,
    first    numeric not null,
    last     numeric not null,
    data     jsonb   not null
);

create table meta_data
(
    curve_id bigint primary key,
    created  timestamp(3) default now(),
    data     jsonb not null
);

create table segments
(
    id       bigserial primary key,
    created  timestamp(3) default now(),
    curve_id bigint  not null,
    scale    int     not null,
    first    numeric not null,
    last     numeric not null,
    data     jsonb   not null

);

create index on items(curve_id);

create index on meta_data(curve_id);

create index on segments(curve_id);