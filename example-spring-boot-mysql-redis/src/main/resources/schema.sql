create table if not exists user_profile (
    user_id varchar(64) not null primary key,
    profile_json text not null,
    key_version bigint not null
);

insert into user_profile (user_id, profile_json, key_version)
values ('user:1001', '{"name":"alice","email":"alice@example.com"}', 1)
on duplicate key update
    profile_json = values(profile_json),
    key_version = values(key_version);
