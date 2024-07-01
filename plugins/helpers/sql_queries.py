class SqlQueries:
    songplay_table_insert = """--sql
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

    users_table_insert = """--sql
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """

    songs_table_insert = """--sql
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """

    artists_table_insert = """--sql
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """

    time_table_insert = """--sql
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """

    staging_events_table_create = """--sql
        CREATE TABLE IF NOT EXISTS staging_events (
            artist TEXT,
            auth TEXT,
            firstName TEXT,
            gender TEXT,
            itemInSession INT,
            lastName TEXT,
            length FLOAT,
            level TEXT,
            location TEXT,
            method TEXT,
            page TEXT,
            registration BIGINT,
            sessionId INT,
            song TEXT,
            status INT,
            ts BIGINT,
            userAgent TEXT,
            userId INT
        );        
        """
    # num_songs INTEGER,
    # artist_id TEXT,
    # artist_latitude FLOAT,
    # artist_longitude FLOAT,
    # artist_location TEXT,
    # artist_name TEXT,
    # song_id TEXT,
    # title TEXT,
    # duration FLOAT,
    # year INTEGER

    staging_songs_table_create = """--sql
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name VARCHAR(65535),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location VARCHAR(65535),
            song_id varchar(256),
            title VARCHAR(65535),
            duration numeric(18,0),
            "year" int4
        );
    """

    songplays_table_create = """--sql
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INT NOT NULL,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INT,
            location TEXT,
            user_agent TEXT
        );
    """

    users_table_create = """--sql
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender CHAR(1),
            level VARCHAR
        );
    """

    songs_table_create = """--sql
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY,
            title TEXT,
            artist_id VARCHAR NOT NULL,
            year INT,
            duration FLOAT
        );
    """

    artists_table_create = """--sql
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR(65535),
            location VARCHAR(65535),
            latitude FLOAT,
            longitude FLOAT
        );
    """

    time_table_create = """--sql
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        );
    """
