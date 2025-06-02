REGISTER '/opt/pig-0.17.0/lib/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

raw_incidents = LOAD '/data/incidents_*.csv' 
    USING CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
    AS (incident_id:chararray, 
        incident_type:chararray,
        description:chararray,
        latitude:double,
        longitude:double,
        street:chararray,
        city:chararray,
        comuna:chararray,
        severity:int,
        timestamp:chararray,
        source:chararray);

filtered_incidents = FILTER raw_incidents BY 
    incident_id IS NOT NULL AND
    incident_type IS NOT NULL AND
    latitude IS NOT NULL AND
    longitude IS NOT NULL AND
    comuna IS NOT NULL AND
    timestamp IS NOT NULL;

deduped_incidents = DISTINCT filtered_incidents;

standardized_incidents = FOREACH deduped_incidents GENERATE
    incident_id,
    (incident_type == 'accidente' ? 'ACCIDENTE' :
     incident_type == 'atasco' ? 'ATASCO' :
     incident_type == 'construccion' ? 'CONSTRUCCION' :
     incident_type == 'peligro' ? 'PELIGRO' :
     incident_type == 'clima' ? 'CLIMA' :
     'OTRO') AS incident_type_std,
    description,
    latitude,
    longitude,
    street,
    city,
    comuna,
    severity,
    timestamp,
    source;

incidents_with_hour = FOREACH standardized_incidents GENERATE
    *,
    SUBSTRING(timestamp, 11, 2) AS hour;

incidents_by_comuna = GROUP standardized_incidents BY comuna;
comuna_counts = FOREACH incidents_by_comuna GENERATE
    group AS comuna,
    COUNT(standardized_incidents) AS incident_count;

comuna_counts_sorted = ORDER comuna_counts BY incident_count DESC;

incidents_by_type = GROUP standardized_incidents BY incident_type_std;
type_counts = FOREACH incidents_by_type GENERATE
    group AS incident_type,
    COUNT(standardized_incidents) AS incident_count;

type_counts_sorted = ORDER type_counts BY incident_count DESC;

incidents_by_hour = GROUP incidents_with_hour BY hour;
hourly_counts = FOREACH incidents_by_hour GENERATE
    group AS hour,
    COUNT(incidents_with_hour) AS incident_count;

hourly_counts_sorted = ORDER hourly_counts BY hour;

severity_by_comuna = GROUP standardized_incidents BY comuna;
comuna_severity = FOREACH severity_by_comuna GENERATE
    group AS comuna,
    COUNT(standardized_incidents) AS total_incidents,
    AVG(standardized_incidents.severity) AS avg_severity,
    MAX(standardized_incidents.severity) AS max_severity;

comuna_severity_sorted = ORDER comuna_severity BY avg_severity DESC;


incidents_with_rounded_coords = FOREACH standardized_incidents GENERATE
    *,
    ROUND(latitude * 100) / 100 AS lat_rounded,
    ROUND(longitude * 100) / 100 AS lon_rounded;

hotspots = GROUP incidents_with_rounded_coords BY (lat_rounded, lon_rounded);
hotspot_analysis = FOREACH hotspots GENERATE
    FLATTEN(group) AS (latitude, longitude),
    COUNT(incidents_with_rounded_coords) AS incident_count,
    incidents_with_rounded_coords.comuna AS comunas;

significant_hotspots = FILTER hotspot_analysis BY incident_count > 5;
significant_hotspots_sorted = ORDER significant_hotspots BY incident_count DESC;

comuna_type_analysis = GROUP standardized_incidents BY (comuna, incident_type_std);
comuna_type_counts = FOREACH comuna_type_analysis GENERATE
    FLATTEN(group) AS (comuna, incident_type),
    COUNT(standardized_incidents) AS incident_count;

peak_hours = FILTER hourly_counts BY incident_count > 10;
peak_hours_sorted = ORDER peak_hours BY incident_count DESC;

STORE comuna_counts_sorted INTO '/output/incidents_by_comuna' 
    USING PigStorage(',');

STORE type_counts_sorted INTO '/output/incidents_by_type' 
    USING PigStorage(',');

STORE hourly_counts_sorted INTO '/output/incidents_by_hour' 
    USING PigStorage(',');

STORE comuna_severity_sorted INTO '/output/severity_by_comuna' 
    USING PigStorage(',');

STORE significant_hotspots_sorted INTO '/output/traffic_hotspots' 
    USING PigStorage(',');

STORE comuna_type_counts INTO '/output/comuna_type_distribution' 
    USING PigStorage(',');

STORE peak_hours_sorted INTO '/output/peak_hours_analysis' 
    USING PigStorage(',');

STORE standardized_incidents INTO '/output/cleaned_incidents' 
    USING PigStorage(',');