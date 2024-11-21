-- Create Interim table to be loaded by the parquet files retrieved by OCI Dataflow Spark Bigquery Connector
CREATE TABLE INTERIM_GA4 (
    "EVENT_DATE" NUMBER,
    "EVENT_TIMESTAMP" NUMBER(19,0),
    "EVENT_NAME" VARCHAR2(256),
    "EVENT_PARAMS" JSON,
    "EVENT_PREVIOUS_TIMESTAMP" NUMBER(19,0),
    "EVENT_VALUE_IN_USD" BINARY_DOUBLE,
    "EVENT_BUNDLE_SEQUENCE_ID" NUMBER(19,0),
    "EVENT_SERVER_TIMESTAMP_OFFSET" NUMBER(19,0),
    "USER_ID" VARCHAR2(256),
    "USER_PSEUDO_ID" VARCHAR2(256),
    "PRIVACY_INFO" JSON,
    "USER_PROPERTIES" JSON,
    "USER_FIRST_TOUCH_TIMESTAMP" NUMBER(19,0),
    "USER_LTV" JSON,
    "DEVICE" JSON,
    "GEO" JSON,
    "APP_INFO" JSON,
    "TRAFFIC_SOURCE" JSON,
    "STREAM_ID" VARCHAR2(256),
    "PLATFORM" VARCHAR2(256),
    "EVENT_DIMENSIONS" JSON,
    "ECOMMERCE" JSON,
    "ITEMS" JSON,
    "COLLECTED_TRAFFIC_SOURCE" JSON,
    "IS_ACTIVE_USER" NUMBER(1,0),
    "BATCH_EVENT_INDEX" NUMBER(19,0),
    "BATCH_PAGE_ID" NUMBER(19,0),
    "BATCH_ORDERING_ID" NUMBER(19,0),
    "SESSION_TRAFFIC_SOURCE_LAST_CLICK" JSON,
    "PUBLISHER" JSON
)
COLUMN STORE COMPRESS FOR QUERY HIGH ROW LEVEL LOCKING
TABLESPACE "DATA";

COMMENT ON TABLE INTERIM_GA4 IS 'Interim table to be loaded by the parquet files retrieved by OCI Dataflow Spark Bigquery Connector';

-- Create Partitioned table to store the cleansed data to be displayed in Oracle Analytics Cloud or other visualization tools
CREATE TABLE GA4_PARTITIONED (
    EVENT_DATE NUMBER,
    EVENT_TIME TIMESTAMP WITH TIME ZONE,
    USER_FIRST_TOUCH_TIMESTAMP TIMESTAMP WITH TIME ZONE,
    EVENT_NAME VARCHAR2(4000),
    USER_PSEUDO_ID VARCHAR2(4000),
    PLATFORM VARCHAR2(4000),
    GEO_CONTINENT VARCHAR2(4000),
    GEO_COUNTRY VARCHAR2(4000),
    GEO_SUB_CONTINENT VARCHAR2(4000),
    TRAFFIC_SOURCE_NAME VARCHAR2(4000),
    TRAFFIC_SOURCE_MEDIUM VARCHAR2(4000),
    TRAFFIC_SOURCE_SOURCE VARCHAR2(4000),
    DEVICE_CATEGORY VARCHAR2(4000),
    DEVICE_MOBILE_BRAND_NAME VARCHAR2(4000),
    DEVICE_MOBILE_MODEL_NAME VARCHAR2(4000),
    DEVICE_OPERATING_SYSTEM VARCHAR2(4000),
    DEVICE_OPERATING_SYSTEM_VERSION VARCHAR2(4000),
    DEVICE_LANGUAGE VARCHAR2(4000),
    DEVICE_IS_LIMITED_AD_TRACKING VARCHAR2(4000),
    DEVICE_BROWSER VARCHAR2(4000),
    DEVICE_BROWSER_VERSION VARCHAR2(4000),
    DEVICE_WEB_INFO_BROWSER VARCHAR2(4000),
    DEVICE_WEB_INFO_BROWSER_VERSION VARCHAR2(4000),
    DEVICE_WEB_INFO_HOSTNAME VARCHAR2(4000),
    MAIN_CATEGORY VARCHAR2(4000),
    ARTICLE_TITLE VARCHAR2(4000),
    SUB_CATEGORY VARCHAR2(4000),
    CAMPAIGN VARCHAR2(4000),
    ENGAGED_SESSION_EVENT VARCHAR2(4000),
    ENTRANCES NUMBER,
    GA_SESSION_ID NUMBER,
    GA_SESSION_NUMBER NUMBER,
    EVENT_MEDIUM VARCHAR2(4000),
    MEMBER_ID_CUSTOM VARCHAR2(4000),
    PAGE_REFERRER VARCHAR2(4000),
    PAGE_LOCATION VARCHAR2(4000),
    ENGAGEMENT_TIME_MSEC NUMBER,
    POST_ID_CUSTOM VARCHAR2(4000),
    USER_SESSION VARCHAR2(4000)
)
PARTITION BY RANGE (EVENT_DATE)
INTERVAL (1)
(PARTITION p0 VALUES LESS THAN (20240901))
COLUMN STORE COMPRESS FOR QUERY HIGH ROW LEVEL LOCKING
TABLESPACE "DATA";

COMMENT ON TABLE GA4_PARTITIONED IS 'Partitioned table to store the cleansed data to be displayed in Oracle Analytics Cloud or other visualization tools';

-- Create a function to return millisecond to timestamp
CREATE OR REPLACE FUNCTION unix_ms_to_timestamp(p_unix_time IN NUMBER)
RETURN TIMESTAMP
IS
BEGIN
  RETURN TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + 
         NUMTODSINTERVAL(p_unix_time / 1000, 'SECOND');
END;
/

-- Create a function to return microsecond to timestamp
CREATE OR REPLACE FUNCTION unix_micro_to_timestamp(p_unix_time IN NUMBER)
RETURN TIMESTAMP
IS
BEGIN
  RETURN TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + 
         NUMTODSINTERVAL(p_unix_time / 1000000, 'SECOND');
END;
/

-- Create a function to return microsecond to timestamp with timezone
CREATE OR REPLACE FUNCTION unix_micro_to_timestamp_tz(
    p_unix_time IN NUMBER,
    p_time_zone IN VARCHAR2 DEFAULT 'UTC'
)
RETURN TIMESTAMP WITH TIME ZONE
IS
BEGIN
    RETURN FROM_TZ(
        TIMESTAMP '1970-01-01 00:00:00' + 
        NUMTODSINTERVAL(p_unix_time / 1000000, 'SECOND'),
        p_time_zone
    );
END unix_micro_to_timestamp_tz;
/

-- Append the data from interim table to partitioned table with Parallel hint
INSERT /*+ APPEND PARALLEL(GA4_PARTITIONED, 4) */ INTO GA4_PARTITIONED
SELECT /*+ PARALLEL(p1, 4) */
    p1.id,
    p1.event_date,
    unix_micro_to_timestamp_tz(p1.event_timestamp, 'Asia/Hong_Kong') AS event_time,
    unix_micro_to_timestamp_tz(p1.user_first_touch_timestamp, 'Asia/Hong_Kong') AS user_first_touch_timestamp,
    p1.event_name,
    p1.user_pseudo_id,
    p1.platform,
    ga4_geo.continent AS geo_continent,
    ga4_geo.country AS geo_country,
    ga4_geo.sub_continent AS geo_sub_continent,
    ga4_traffic_source.name AS traffic_source_name,
    ga4_traffic_source.medium AS traffic_source_medium,
    ga4_traffic_source.source AS traffic_source_source,
    ga4_device.category AS device_category,
    ga4_device.mobile_brand_name AS device_mobile_brand_name,
    ga4_device.mobile_model_name AS device_mobile_model_name,
    ga4_device.operating_system AS device_operating_system,
    ga4_device.operating_system_version AS device_operating_system_version,
    ga4_device.language AS device_language,
    ga4_device.is_limited_ad_tracking AS device_is_limited_ad_tracking,
    ga4_device.browser AS device_browser,
    ga4_device.browser_version AS device_browser_version,
    ga4_device.webinfo_browser AS device_web_info_browser,
    ga4_device.webinfo_browser_version AS device_web_info_browser_version,
    ga4_device.webinfo_hostname AS device_web_info_hostname,
    MAX(CASE WHEN ga4_event_params.key = 'main_category_custom' THEN ga4_event_params.string_value END) as main_category,
    MAX(CASE WHEN ga4_event_params.key = 'article_title' THEN ga4_event_params.string_value END) as article_title,
    MAX(CASE WHEN ga4_event_params.key = 'sub_category_custom' THEN ga4_event_params.string_value END) as sub_category,
    MAX(CASE WHEN ga4_event_params.key = 'campaign' THEN ga4_event_params.string_value END) as campaign,
    MAX(CASE WHEN ga4_event_params.key = 'engaged_session_event' THEN ga4_event_params.string_value END) as engaged_session_event,
    MAX(CASE WHEN ga4_event_params.key = 'entrances' THEN ga4_event_params.int_value END) as entrances,
    MAX(CASE WHEN ga4_event_params.key = 'ga_session_id' THEN ga4_event_params.int_value END) as ga_session_id,
    MAX(CASE WHEN ga4_event_params.key = 'ga_session_number' THEN ga4_event_params.int_value END) as ga_session_number,
    MAX(CASE WHEN ga4_event_params.key = 'medium' THEN ga4_event_params.string_value END) as event_medium,
    MAX(CASE WHEN ga4_event_params.key = 'member_id_custom' THEN ga4_event_params.string_value END) as member_id_custom,
    MAX(CASE WHEN ga4_event_params.key = 'page_referrer' THEN ga4_event_params.string_value END) as page_referrer,
    MAX(CASE WHEN ga4_event_params.key = 'page_location' THEN ga4_event_params.string_value END) as page_location,
    MAX(CASE WHEN ga4_event_params.key = 'engagement_time_msec' THEN ga4_event_params.int_value END) as engagement_time_msec,
    MAX(CASE WHEN ga4_event_params.key = 'post_id_custom' THEN NVL(ga4_event_params.string_value,'') END) as post_id_custom,
    p1.user_pseudo_id || MAX(CASE WHEN ga4_event_params.key = 'ga_session_id' THEN ga4_event_params.string_value END) as user_session
FROM INTERIM_GA4 p1
    CROSS JOIN LATERAL (
        SELECT *
        FROM json_table(p1.geo, '$' 
            COLUMNS (
                continent VARCHAR2(4000 CHAR) path '$.continent',
                country VARCHAR2(4000 CHAR) path '$.country',
                sub_continent VARCHAR2(4000 CHAR) path '$.sub_continent'
            )
        )
    ) ga4_geo
    CROSS JOIN LATERAL (
        SELECT *
        FROM json_table(p1.device, '$' 
            COLUMNS (
                category VARCHAR2(4000 CHAR) path '$.category',
                is_limited_ad_tracking VARCHAR2(4000 CHAR) path '$.is_limited_ad_tracking',
                browser VARCHAR2(4000 CHAR) path '$.browser',
                browser_version VARCHAR2(4000 CHAR) path '$.browser_version',
                language VARCHAR2(4000 CHAR) path '$.language',
                mobile_brand_name VARCHAR2(4000 CHAR) path '$.mobile_brand_name',
                mobile_model_name VARCHAR2(4000 CHAR) path '$.mobile_model_name',
                operating_system VARCHAR2(4000 CHAR) path '$.operating_system',
                operating_system_version VARCHAR2(4000 CHAR) path '$.operating_system_version',
                webinfo_browser VARCHAR2(4000 CHAR) path '$.webinfo.browser',
                webinfo_browser_version VARCHAR2(4000 CHAR) path '$.webinfo.browser_version',
                webinfo_hostname VARCHAR2(4000 CHAR) path '$.webinfo.hostname'
            )
        )
    ) ga4_device
    CROSS JOIN LATERAL (
        SELECT *
        FROM json_table(p1.traffic_source, '$'
            COLUMNS (
                medium VARCHAR2(128 CHAR) path '$.medium',
                name VARCHAR2(128 CHAR) path '$.name',
                source VARCHAR2(32 CHAR) path '$.source'
            )
        )
    ) ga4_traffic_source
    CROSS JOIN LATERAL (
        SELECT key, string_value, int_value
        FROM json_table(p1.event_params, '$[*]'
            COLUMNS (
                key VARCHAR2(4000 CHAR) path '$.key',
                string_value VARCHAR2(4000 CHAR) path '$.value.string_value',
                int_value NUMBER path '$.value.int_value'
            )
        )
    ) ga4_event_params
GROUP BY p1.event_date, p1.event_name, p1.event_timestamp, p1.user_first_touch_timestamp, p1.user_pseudo_id, p1.platform,
    ga4_geo.continent, ga4_geo.country, ga4_geo.sub_continent,
    ga4_traffic_source.name, ga4_traffic_source.medium, ga4_traffic_source.source,
    ga4_device.category, ga4_device.mobile_brand_name, ga4_device.mobile_model_name,
    ga4_device.operating_system, ga4_device.operating_system_version, ga4_device.language,
    ga4_device.is_limited_ad_tracking, ga4_device.browser, ga4_device.browser_version,
    ga4_device.webinfo_browser, ga4_device.webinfo_browser_version, ga4_device.webinfo_hostname

