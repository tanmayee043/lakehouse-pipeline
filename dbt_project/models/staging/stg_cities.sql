select
    {{ city_id('city') }} as city_id,
    trim(city) as city_name,
    trim(country) as country,
    cast(population as bigint) as population,
    cast(lat as double) as latitude,
    cast(lng as double) as longitude
from {{ source('raw', 'cities') }}
