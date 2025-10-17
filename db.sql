-- table columns and info 

create table some_schema.youtube_videos ( 
    video_id VARCHAR(20) PRIMARY KEY, 
    title TEXT NOT NULL, 
    channel_title TEXT, 
    category_id INT, 
    publish_time TIMESTAMP, 
    tags TEXT, 
    thumbnail_link TEXT, description TEXT, 
    comments_disabled BOOLEAN, 
    ratings_disabled BOOLEAN, 
    video_error_or_removed BOOLEAN,
    recorded_at DATE DEFAULT CURRENT_DATE
);

CREATE TABLE some_schema.youtube_trending_history ( 
    id SERIAL PRIMARY KEY, 
    video_id VARCHAR(20) REFERENCES youtube_videos(video_id) ON DELETE CASCADE, 
    publish_date DATE NOT NULL, 
    views BIGINT, 
    likes BIGINT, 
    dislikes BIGINT, 
    comment_count BIGINT, 
    region VARCHAR(5) DEFAULT 'US', 
    recorded_at DATE DEFAULT CURRENT_DATE
    );

ALTER TABLE some_schema.youtube_trending_history
ADD CONSTRAINT unique_trend UNIQUE (video_id, publish_date, region);

-- pipeline build

create table some_schema.youtube_videos_p
(
    video_id       varchar(20) not null
        primary key,
    title          text        not null,
    channel_title  text,
    category_id    integer,
    publish_date   timestamp,
    tags           text,
    thumbnail_link text,
    recorded_at    date default CURRENT_DATE,
    views          bigint,
    likes          bigint,
    comment_count  bigint
);

CREATE TABLE some_schema.youtube_trending_history_p
(
    id            serial
        primary key,
    video_id      varchar(20)
        references youtube_videos_p
            on delete cascade,
    publish_date date not null,
    views         bigint,
    likes         bigint,
    comment_count bigint,
    recorded_at   date default CURRENT_DATE,
    constraint youtube_trending_history_p_pk
        unique (video_id, recorded_at)
);