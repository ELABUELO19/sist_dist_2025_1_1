-- advanced_traffic_analysis.pig
-- Advanced Apache Pig scripts for comprehensive traffic analysis

-- Register additional UDFs and libraries
REGISTER '/opt/pig-0.17.0/lib/piggybank.jar';
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Load the processed incidents data
incidents = LOAD '/data/processed_incidents_*.csv' 
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

-- Clean and prepare data
clean_incidents = FILTER incidents BY 
    incident_id IS NOT NULL AND
    incident_type IS NOT NULL AND
    latitude IS NOT NULL AND
    longitude IS NOT NULL AND
    comuna IS NOT NULL;

-- Add derived fields for analysis
enriched_incidents = FOREACH clean_incidents GENERATE
    *,
    SUBSTRING(timestamp, 11, 2) AS hour,
    SUBSTRING(timestamp, 8, 2) AS day,
    SUBSTRING(timestamp, 5, 2) AS month,
    SUBSTRING(timestamp, 0, 4) AS year,
    (severity >= 7 ? 'HIGH' : 
     severity >= 4 ? 'MEDIUM' : 'LOW') AS severity_category;

-- ========== COMPREHENSIVE ANALYSIS QUERIES ==========

-- 1. TEMPORAL PATTERN ANALYSIS
-- Peak hours analysis by day of week
temporal_patterns = GROUP enriched_incidents BY (hour);
hourly_distribution = FOREACH temporal_patterns GENERATE
    group AS hour,
    COUNT(enriched_incidents) AS total_incidents,
    AVG(enriched_incidents.severity) AS avg_severity,
    COUNT(FILTER enriched_incidents BY severity >= 7) AS high_severity_count;

-- Order by hour for chronological view
hourly_sorted = ORDER hourly_distribution BY hour;

-- Daily patterns
daily_patterns = GROUP enriched_incidents BY day;
daily_distribution = FOREACH daily_patterns GENERATE
    group AS day,
    COUNT(enriched_incidents) AS incidents_count,
    AVG(enriched_incidents.severity) AS avg_severity;

-- 2. GEOGRAPHIC HOTSPOT ANALYSIS
-- High-precision geographic clustering
geo_incidents = FOREACH enriched_incidents GENERATE
    ROUND(latitude * 1000) / 1000 AS lat_precise,
    ROUND(longitude * 1000) / 1000 AS lon_precise,
    *;

geographic_clusters = GROUP geo_incidents BY (lat_precise, lon_precise);
hotspot_analysis = FOREACH geographic_clusters GENERATE
    FLATTEN(group) AS (latitude, longitude),
    COUNT(geo_incidents) AS incident_density,
    AVG(geo_incidents.severity) AS avg_severity,
    MAX(geo_incidents.severity) AS max_severity,
    COUNT(FILTER geo_incidents BY severity >= 7) AS critical_incidents,
    FLATTEN(DISTINCT geo_incidents.comuna) AS affected_comunas;

-- Filter for significant hotspots
significant_hotspots = FILTER hotspot_analysis BY incident_density >= 3;
hotspots_ranked = ORDER significant_hotspots BY incident_density DESC;

-- 3. COMUNA PERFORMANCE METRICS
comuna_metrics = GROUP enriched_incidents BY comuna;
comuna_analysis = FOREACH comuna_metrics GENERATE
    group AS comuna,
    COUNT(enriched_incidents) AS total_incidents,
    AVG(enriched_incidents.severity) AS avg_severity,
    COUNT(FILTER enriched_incidents BY severity >= 7) AS critical_incidents,
    COUNT(FILTER enriched_incidents BY incident_type == 'ACCIDENTE') AS accidents,
    COUNT(FILTER enriched_incidents BY incident_type == 'ATASCO') AS traffic_jams,
    COUNT(FILTER enriched_incidents BY incident_type == 'CONSTRUCCION') AS construction,
    (COUNT(FILTER enriched_incidents BY severity >= 7) * 100.0 / COUNT(enriched_incidents)) AS critical_rate;

-- Rank comunas by different metrics
comunas_by_incidents = ORDER comuna_analysis BY total_incidents DESC;
comunas_by_severity = ORDER comuna_analysis BY avg_severity DESC;
comunas_by_critical_rate = ORDER comuna_analysis BY critical_rate DESC;

-- 4. INCIDENT TYPE ANALYSIS
type_analysis = GROUP enriched_incidents BY incident_type;
incident_type_metrics = FOREACH type_analysis GENERATE
    group AS incident_type,
    COUNT(enriched_incidents) AS frequency,
    AVG(enriched_incidents.severity) AS avg_severity,
    COUNT(FILTER enriched_incidents BY severity >= 7) AS critical_count,
    COUNT(DISTINCT enriched_incidents.comuna) AS affected_comunas;

-- 5. SEVERITY DISTRIBUTION ANALYSIS
severity_distribution = GROUP enriched_incidents BY severity_category;
severity_stats = FOREACH severity_distribution GENERATE
    group AS severity_level,
    COUNT(enriched_incidents) AS incident_count,
    (COUNT(enriched_incidents) * 100.0 / (double)COUNT(clean_incidents)) AS percentage;

-- 6. CROSS-DIMENSIONAL ANALYSIS
-- Comuna-Type-Hour analysis for detailed patterns
complex_grouping = GROUP enriched_incidents BY (comuna, incident_type, hour);
detailed_patterns = FOREACH complex_grouping GENERATE
    FLATTEN(group) AS (comuna, incident_type, hour),
    COUNT(enriched_incidents) AS incident_count,
    AVG(enriched_incidents.severity) AS avg_severity;

-- Filter for significant patterns
significant_patterns = FILTER detailed_patterns BY incident_count >= 2;
patterns_sorted = ORDER significant_patterns BY incident_count DESC;

-- 7. RISK ASSESSMENT SCORING
-- Calculate risk scores for each comuna
risk_scoring = FOREACH comuna_analysis GENERATE
    comuna,
    total_incidents,
    avg_severity,
    critical_incidents,
    (total_incidents * 0.3 + avg_severity * 10 + critical_incidents * 2) AS risk_score;

risk_ranked = ORDER risk_scoring BY risk_score DESC;

-- 8. EFFICIENCY METRICS
-- Response time proxy based on incident clustering
efficiency_analysis = FOREACH hotspots_ranked GENERATE
    latitude,
    longitude,
    incident_density,
    avg_severity,
    (incident_density * avg_severity) AS response_priority_score;

-- 9. TREND ANALYSIS
-- Weekly trends (simplified - would need more sophisticated date handling for real trends)
trend_analysis = GROUP enriched_incidents BY (month, day);
trend_metrics = FOREACH trend_analysis GENERATE
    FLATTEN(group) AS (month, day),
    COUNT(enriched_incidents) AS daily_incidents,
    AVG(enriched_incidents.severity) AS daily_avg_severity;

-- 10. SAFETY INDEX CALCULATION
-- Calculate safety index per comuna (inverse of risk)
safety_index = FOREACH risk_ranking GENERATE
    comuna,
    risk_score,
    (100.0 - (risk_score * 100.0 / MAX(risk_ranking.risk_score))) AS safety_index;

-- ========== STORE COMPREHENSIVE RESULTS ==========

-- Core Analysis Results
STORE hourly_sorted INTO '/output/temporal/hourly_patterns' 
    USING PigStorage(',');

STORE daily_distribution INTO '/output/temporal/daily_patterns' 
    USING PigStorage(',');

STORE hotspots_ranked INTO '/output/geographic/traffic_hotspots_detailed' 
    USING PigStorage(',');

STORE comunas_by_incidents INTO '/output/comuna/ranking_by_incidents' 
    USING PigStorage(',');

STORE comunas_by_severity INTO '/output/comuna/ranking_by_severity' 
    USING PigStorage(',');

STORE comunas_by_critical_rate INTO '/output/comuna/ranking_by_critical_rate' 
    USING PigStorage(',');

STORE incident_type_metrics INTO '/output/types/detailed_type_analysis' 
    USING PigStorage(',');

STORE severity_stats INTO '/output/severity/distribution_analysis' 
    USING PigStorage(',');

-- Advanced Analysis Results
STORE significant_patterns INTO '/output/advanced/multi_dimensional_patterns' 
    USING PigStorage(',');

STORE risk_ranked INTO '/output/risk/comuna_risk_assessment' 
    USING PigStorage(',');

STORE efficiency_analysis INTO '/output/efficiency/response_priority_zones' 
    USING PigStorage(',');

STORE trend_metrics INTO '/output/trends/temporal_trends' 
    USING PigStorage(',');

STORE safety_index INTO '/output/safety/comuna_safety_index' 
    USING PigStorage(',');

-- Summary Statistics
summary_stats = FOREACH (GROUP clean_incidents ALL) GENERATE
    COUNT(clean_incidents) AS total_processed_incidents,
    AVG(clean_incidents.severity) AS overall_avg_severity,
    COUNT(FILTER clean_incidents BY severity >= 7) AS total_critical_incidents,
    COUNT(DISTINCT clean_incidents.comuna) AS total_affected_comunas,
    COUNT(DISTINCT clean_incidents.incident_type) AS total_incident_types;

STORE summary_stats INTO '/output/summary/overall_statistics' 
    USING PigStorage(',');

-- Export cleaned data for potential further analysis
STORE enriched_incidents INTO '/output/processed/enriched_incident_data' 
    USING PigStorage(',');