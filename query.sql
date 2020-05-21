select name, hour, high_per_hr, ts
from (
select name, hour, high_per_hr, ts, rn
from (
   select db3.name, db3.hour, db3.high_per_hr, db4.ts, ROW_NUMBER() OVER (PARTITION BY db3.name,db3.hour ORDER BY db4.ts) as rn
   from (
      select name, hour, MAX(high) as high_per_hr
      from (
          select
              name,
              high,
              ts,
              EXTRACT(HOUR FROM date_parse(ts,'%Y-%m-%d %H:%i:%S')) as hour
          from project_delivery_stream_s3
      ) db2
      GROUP BY name, hour
      ORDER BY name, hour
   )db3
   LEFT JOIN (
       select
           name,
           high,
           ts,
           EXTRACT(HOUR FROM date_parse(ts,'%Y-%m-%d %H:%i:%S')) as hour
       FROM project_delivery_stream_s3
       ) AS db4
          ON db3.high_per_hr = db4.high and db3.hour = db4.hour
   GROUP BY db3.name, db3.hour, db3.high_per_hr, db4.ts
   ORDER BY db3.name, db3.hour, db3.high_per_hr, db4.ts)db5
where rn = 1) db6;