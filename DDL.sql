CREATE SCHEMA usr;
CREATE SCHEMA video;
CREATE SCHEMA danmu;

create table usr.users(
    mid bigint PRIMARY KEY,
    name varchar(255),
    sex varchar(10),
    birthday varchar(20),
    level int,
    sign text,
    identity varchar(20)
);

create table video.videos(
    BV varchar(255) PRIMARY KEY,
    title varchar(255),
    owner_mid bigint,
    commit_time timestamp,
    review_time timestamp,
    public_time timestamp,
    duration interval,
    description text,
    reviewer bigint
);

create table danmu.danmu(
	id bigserial PRIMARY KEY,
    BV varchar(255) not null,
    mid bigint,
    time interval,
    content text
);

create table usr.followings(
	id bigserial PRIMARY KEY,
    follower_id bigint not null,
    followed_id bigint
);

create table video.likes(
	id bigserial PRIMARY KEY,
	who_likes bigint not null,
	BV varchar(255)
);

create table video.coin(
	id bigserial PRIMARY KEY,
    who_coins bigint not null,
    BV varchar(255)
);

create table video.favorite(
	id bigserial PRIMARY KEY,
    who_favorites bigint not null,
    BV varchar(255)
);

create table video.view(
    id bigserial PRIMARY KEY,
    who_views bigint not null,
    BV varchar(255),
    last_time interval
);