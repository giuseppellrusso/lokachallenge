CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS events_partitions;

CREATE TABLE IF NOT EXISTS core.vehicles_events (
    event varchar(12),
    event_at timestamp,
    organization_id varchar(6),
    vehicle_id varchar(37),
    latitude float,
    longitude float,
    rank int,
    operating_period_id varchar(4),
    is_first_update Boolean,
    is_last_update Boolean,
    is_first_op_update Boolean,
    is_last_op_update Boolean,
    event_date date,
    _last_updated_at timestamp
) PARTITION BY LIST(event_date);

CREATE TABLE IF NOT EXISTS core.vehicles (
    vehicle_id varchar(37) PRIMARY KEY,
    organization_id varchar(6),
    registered_at timestamp,
    deregistered_at timestamp,
    first_location_update_at timestamp,
    first_location_latitude float,
    first_location_longitude float,
    last_location_update_at timestamp,
    last_location_latitude float,
    last_location_longitude float,
    _last_updated_at timestamp
);

CREATE TABLE IF NOT EXISTS core.operating_periods (
    operating_period_id varchar(4) PRIMARY KEY,
    organization_id varchar(6),
    created_at timestamp,
    started_at timestamp,
    ended_at timestamp,
    _last_updated_at timestamp
);

CREATE TABLE IF NOT EXISTS marts.vehicles_op_stats(
    vehicle_id varchar(37),
    operating_period_id varchar(4),
    first_op_update_at timestamp,
    first_op_update_latitude float,
    first_op_update_longitude float,
    last_op_update_at timestamp,
    last_op_update_latitude float,
    last_op_update_longitude float,
    distance_km float,
    _last_updated_at timestamp,
    CONSTRAINT composite_primary_key PRIMARY KEY(vehicle_id, operating_period_id)
);


/*::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
/*::                                                                         :*/
/*::  This routine calculates the distance between two points (given the     :*/
/*::  latitude/longitude of those points). It is being used to calculate     :*/
/*::  the distance between two locations using GeoDataSource(TM) Products    :*/
/*::                                                                         :*/
/*::  Definitions:                                                           :*/
/*::    South latitudes are negative, east longitudes are positive           :*/
/*::                                                                         :*/
/*::  Passed to function:                                                    :*/
/*::    lat1, lon1 = Latitude and Longitude of point 1 (in decimal degrees)  :*/
/*::    lat2, lon2 = Latitude and Longitude of point 2 (in decimal degrees)  :*/
/*::    unit = the unit you desire for results                               :*/
/*::           where: 'M' is statute miles (default)                         :*/
/*::                  'K' is kilometers                                      :*/
/*::                  'N' is nautical miles                                  :*/
/*::  Worldwide cities and other features databases with latitude longitude  :*/
/*::  are available at https://www.geodatasource.com                         :*/
/*::                                                                         :*/
/*::  For enquiries, please contact sales@geodatasource.com                  :*/
/*::                                                                         :*/
/*::  Official Web site: https://www.geodatasource.com                       :*/
/*::                                                                         :*/
/*::  Thanks to Kirill Bekus for contributing the source code.               :*/
/*::                                                                         :*/
/*::         GeoDataSource.com (C) All Rights Reserved 2022                  :*/
/*::                                                                         :*/
/*::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/

CREATE OR REPLACE FUNCTION marts.calculate_distance(lat1 float, lon1 float, lat2 float, lon2 float, units varchar)
RETURNS float AS $dist$
    DECLARE
        dist float = 0;
        radlat1 float;
        radlat2 float;
        theta float;
        radtheta float;
    BEGIN
        IF lat1 = lat2 AND lon1 = lon2
            THEN RETURN dist;
        ELSE
            radlat1 = pi() * lat1 / 180;
            radlat2 = pi() * lat2 / 180;
            theta = lon1 - lon2;
            radtheta = pi() * theta / 180;
            dist = sin(radlat1) * sin(radlat2) + cos(radlat1) * cos(radlat2) * cos(radtheta);

            IF dist > 1 THEN dist = 1; END IF;

            dist = acos(dist);
            dist = dist * 180 / pi();
            dist = dist * 60 * 1.1515;

            IF units = 'K' THEN dist = dist * 1.609344; END IF;
            IF units = 'N' THEN dist = dist * 0.8684; END IF;

            RETURN dist;
        END IF;
    END;
$dist$ LANGUAGE plpgsql;




CREATE OR REPLACE PROCEDURE marts.model_vehicles_op_stats(start_date date default '1900-01-01', end_date date default '2100-01-01')
LANGUAGE sql AS $procedure$


with first_op_update as (
	select 
		vehicle_id, 
		operating_period_id, 
		event_at as first_op_update_at,
		latitude as first_op_update_latitude,
		longitude as first_op_update_longitude
	from core.vehicles_events
	where is_first_op_update
	and event_date >= start_date and event_date <= end_date
),
last_op_update as (
	select 
		vehicle_id, 
		operating_period_id, 
		event_at as last_op_update_at,
		latitude as last_op_update_latitude,
		longitude as last_op_update_longitude
	from core.vehicles_events 
	where is_last_op_update
	and event_date >= start_date and event_date <= end_date
)
insert into marts.vehicles_op_stats
select 
	first.vehicle_id,
	first.operating_period_id,
	first.first_op_update_at,
	first.first_op_update_latitude,
	first.first_op_update_longitude,
	last.last_op_update_at,
	last.last_op_update_latitude,
	last.last_op_update_longitude,
	marts.calculate_distance(first_op_update_latitude, first_op_update_longitude, last_op_update_latitude, last_op_update_longitude, 'K') as distance_km,
	current_timestamp as _last_updated_at
from first_op_update as first
inner join last_op_update as last on (first.vehicle_id = last.vehicle_id and first.operating_period_id = last.operating_period_id)
ON CONFLICT ON CONSTRAINT composite_primary_key 
DO UPDATE SET vehicle_id = excluded.vehicle_id, 
    operating_period_id = excluded.operating_period_id, 
    first_op_update_at = excluded.first_op_update_at, 
    first_op_update_latitude = excluded.first_op_update_latitude, 
    first_op_update_longitude = excluded.first_op_update_longitude, 
    last_op_update_at = excluded.last_op_update_at, 
    last_op_update_latitude = excluded.last_op_update_latitude, 
    last_op_update_longitude = excluded.last_op_update_longitude, 
    distance_km = excluded.distance_km, 
    _last_updated_at = current_timestamp;


$procedure$;