create table items
(
    id       bigserial primary key,
    created  timestamp(3) default now(),
    curve_id bigint  not null,
    first    numeric not null,
    last     numeric not null,
    data     jsonb   not null
);

create table info
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

create table perform_cache_state
(
  curve_id bigint primary key,
  state timestamp(3),
  well_id varchar(256)
);

create index on items(curve_id);
create index on items(curve_id, first, last);

create index on info(curve_id);

create index on segments(curve_id);
create index on segments(curve_id, scale);
create index on segments(curve_id, scale, first, last);

create index on perform_cache_state(curve_id);