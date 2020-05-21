select name, hour, MAX(high)
from (
    select
        name,
        high,
        EXTRACT(HOUR FROM date_parse(ts,'%Y-%m-%d %H:%i:%S')) as hour
    from project_delivery_stream_s3
) project_delivery_stream_s3
GROUP BY name, hour
ORDER BY name, hour;