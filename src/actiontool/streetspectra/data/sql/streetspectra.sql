-- ###########################################################################
-- Auxiliar database to avoid duplicate observations into the ACTION Database
-- ###########################################################################

-- ===================================================================
--            ZOONIVERSE CLASSIFICATIONS MANAGMENT TABLES
-- ===================================================================

------------------------------------------------------------------------
-- This is the current Zooniverse export data format
-- This table is generic for all Zooniverse projects
-- Some columns are complex like metadata, annotations and subject_data
-- which containes nested information
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zoo_export_t
(
    classification_id   INTEGER,
    user_name           TEXT,    -- only for registered users
    user_id             INTEGER, -- only for registered users
    user_ip             TEXT,
    workflow_id         INTEGER,    
    workflow_name       TEXT,   
    workflow_version    TEXT,
    created_at          TEXT,   
    gold_standard       TEXT,   -- JSON string  
    expert              TEXT,   -- JSON string
    metadata            TEXT,   -- long JSON string with deep nested info
    annotations         TEXT,   -- long JSON string with deep nested info
    subject_data        TEXT,   -- long JSON string with deep nested info
    subject_ids         TEXT,   -- JSON string

    PRIMARY KEY(classification_id)
);

----------------------------------------------------------------------
-- This table keeps track of Zooniverse export runs
-- If we run a classification by Airflow backfilling
-- we may loose track of a window of classifications not dumped to the
-- ACTION database.
-- This history log helps identify when and provides info to fix it.
---------------------------------------------------------------------- 

CREATE TABLE IF NOT EXISTS zoo_export_window_t
(
    executed_at         TEXT,   -- execution timestamp
    before              TEXT,   -- lastest classification timestamp before insertion
    after               TEXT,   -- lastest classification timestamp after insertion
    PRIMARY KEY(executed_at)
);

------------------------------------------------------------------------
-- This is the table where we extract all StreetSpectra relevant data 
-- from Zooniverse individual classifications entries in the export file
-- in order to make final aggregate classifications
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS spectra_classification_t
(
    classification_id   INTEGER, -- unique Zooinverse classification identifier
    subject_id          INTEGER, -- Zooinverse image id subject of classification
    workflow_id         INTEGER, -- Workflow that produced this classification
    user_id             INTEGER, -- not NULL for registered users, NULL for anonymous users
    user_ip             TEXT,    -- used only in case of anonymous users
    started_at          TEXT,    -- Classification UTC start timestamp, IS08601 format
    finished_at         TEXT,    -- Classification UTC end timestamp, IS08601 format
    width               INTEGER, -- image width
    height              INTEGER, -- image height
    -- Metadata coming from the observation platform (i.e Epicollect 5)
    image_id            INTEGER, -- observing platform image Id
    image_url           TEXT,    -- observing platform image URL
    image_long          REAL,    -- image aprox. longitude
    image_lat           REAL,    -- image aprox. latitude
    image_observer      TEXT,    -- observer nickname, if any
    image_comment       TEXT,    -- image optional comment
    image_source        TEXT,    -- observing platform name (currently "Epicollect 5")
    image_created_at    TEXT,    -- image creation UTC timestamp, ISO8601 format
    image_spectrum      TEXT,    -- spectrum type, if any, given by observer to his intended target (which we really don't know)

    PRIMARY KEY(classification_id)
);


CREATE TABLE IF NOT EXISTS light_sources_t
(
    classification_id   INTEGER, -- unique Zooinverse classification identifier
    cluster_id          INTEGER, -- light source identifier pointed to by user within the subject. Initially NULL
    epsilon             REAL,    -- DBSCAN's epsilon pàrameter. The maximum distance between two samples for one to be considered as in the neighborhood of the other. 
    source_x            REAL,    -- light source x coordinate within the image
    source_y            REAL,    -- light source y coordinate within the image
    spectrum_type       TEXT,    -- spectrum type ('HPS','MV','LED','MH')    
    aggregated          INTEGER, -- 1 if passed through the aggregation process, NULL otherwise

    FOREIGN KEY(classification_id)     REFERENCES spectra_classification_t(classification_id)
);

CREATE VIEW IF NOT EXISTS spectra_classification_v
AS SELECT
    s.classification_id,
    s.subject_id , 
    s.workflow_id, 
    s.user_id,  
    s.user_ip, 
    s.started_at, 
    s.finished_at, 
    s.width,  
    s.height, 
    s.image_id, 
    s.image_url, 
    s.image_long, 
    s.image_lat, 
    s.image_observer, 
    s.image_comment, 
    s.image_source,  
    s.image_created_at, 
    s.image_spectrum,
    l.cluster_id,
    l.epsilon,
    l.source_x,
    l.source_y,
    l.spectrum_type,
    l.aggregated
FROM spectra_classification_t AS s
JOIN light_sources_t AS l USING(classification_id);

------------------------------------------------------------------------
-- This is the table where we store all StreetSpectra aggregate
-- classifications data ready to be exported to a suitable file format
-- to Zenodo 
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS spectra_aggregate_t
(
    subject_id          INTEGER, -- Zooinverse image id subject of classification
    cluster_id          INTEGER, -- light source identifier pointed to by user within the subject.
    width               INTEGER, -- image width
    height              INTEGER, -- image height
    source_x            REAL,    -- average light source x coordinate within the image
    source_y            REAL,    -- average light source y coordinate within the image
    spectrum_type       TEXT,    -- spectrum type mode (statistics), One of ('LED', 'MV', 'HPS', 'LPS', 'MH') or NULL
    spectrum_absfreq    INTEGER, -- absolute frequency for the spectrum statistics mode
    spectrum_distr      TEXT,    -- classification distribution made by the users for a given light source
    cluster_size        INTEGER, -- Number of individual (x,y) points that belongs to the same light source, treated as a cluster 
    epsilon             REAL,    -- DBSCAN's epsilon pàrameter. The maximum distance between two samples for one to be considered as in the neighborhood of the other. 
    rejection_tag       TEXT,    -- When spectrum_type is NULL, shows the reason why
    -- Metadata coming from the observation platform (i.e Epicollect 5)
    image_id            INTEGER, -- observing platform image Id
    image_url           TEXT,    -- observing platform image URL
    image_long          REAL,    -- image aprox. longitude
    image_lat           REAL,    -- image aprox. latitude
    image_observer      TEXT,    -- observer nickname, if any
    image_comment       TEXT,    -- image optional comment
    image_source        TEXT,    -- observing platform name (currently "Epicollect 5")
    image_created_at    TEXT,    -- image creation UTC timestamp in iso 8601 format, with trailing Z
    image_spectrum      TEXT,    -- spectrum type, if any, given by observer to his intended target (which we really don't know)

    PRIMARY KEY(subject_id, cluster_id)
);


------------------------------------------------------------------------
-- This is the table where we store all StreetSpectra aggregate
-- classifications data ready to be exported to a suitable file format
-- to Zenodo 
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zenodo_csv_t
(
    hash        BLOB,    -- CSV hash
    type        TEXT,    -- Either 'individual' or 'aggregated'.
    version     TEXT,    -- Publish version as semantic versioning (YY.MM)

    PRIMARY KEY(hash)
);


-- ===================================================================
--               IMAGE COLLECTION MANAGMENT TABLES
-- ===================================================================

CREATE TABLE IF NOT EXISTS epicollect5_t
(
    image_id            TEXT,    -- Image GUID
    created_at          TEXT,    -- Original entry creation timestamp
    uploaded_at         TEXT,    -- Image upload into database timestamp
    written_at          TEXT,    -- Database insertion timestamp
    title               TEXT,    -- Image title, usually the GUID
    observer            TEXT,    -- observer's nickname
    latitude            REAL,    -- image latitude in degrees
    longitude           REAL,    -- image longitude in degrees
    accuracy            INTEGER, -- coordinates accuracy
    url                 TEXT,    -- image URL
    spectrum_type       TEXT,    -- optional spectrum type set by observer
    comment             TEXT,    -- optional observer comment
    project             TEXT,    -- source project identifier ('street-spectra')
    source              TEXT,    -- Observing platform ('Epicollect5')
    obs_type            TEXT,    -- Entry type ('observation')

    PRIMARY KEY(image_id)
);
