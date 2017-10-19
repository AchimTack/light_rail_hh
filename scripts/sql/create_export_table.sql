DROP TABLE IF EXISTS light_rail_sums;

CREATE TABLE light_rail_sums AS

SELECT 
	a.dep_time,
    a.station,
    sum(a.boarders) as boarders,
    sum(a.deboarders) as deboarders,
	ST_SetSRID(ST_MakePoint(b.lon, b.lat),4326) as geom

FROM 
	public.light_rail_passengers a,
    public.light_rail_stations b
     
WHERE
	a.station = b.station
	
GROUP BY
	a.dep_time,
    a.station,
    geom
;

CREATE INDEX dep_time_idx ON light_rail_sums (dep_time);
CREATE INDEX station_idx ON light_rail_sums (station);
CREATE INDEX geom_idx ON light_rail_sums (geom);