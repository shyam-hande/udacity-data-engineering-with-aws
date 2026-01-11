DROP TABLE IF EXISTS staging_songs;
DROP TABLE IF EXISTS staging_events;

DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS songplays;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS time;

CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_name         VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
);

CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        lastName            VARCHAR,
        gender              CHAR(1),
        itemInSession       INT,
        length              FLOAT,
        level               VARCHAR,
        location            TEXT,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        userAgent           TEXT,
        userId              VARCHAR,
        song                VARCHAR,
        status              INT,
        ts                  BIGINT,
);

CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR     PRIMARY KEY,
        artist_id           VARCHAR,
        title               VARCHAR,
        year                INT,
        duration            FLOAT
);

CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR     PRIMARY KEY,
        name                VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT,
        location            TEXT,
);



CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         VARCHAR     PRIMARY KEY,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        sessionid           INT,
        start_time          TIMESTAMP   NOT NULL,
        userid              VARCHAR,
        level               VARCHAR,
        location            TEXT,
        useragent           TEXT
);

CREATE TABLE IF NOT EXISTS users(
        userid              VARCHAR     PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              CHAR(1),
        level               VARCHAR
);



CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP   PRIMARY KEY,
        hour                INT,
        day                 INT,
        week                INT,
        weekday             INT,
        month               INT,
        year                INT   
);
