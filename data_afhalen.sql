SELECT count(id)
FROM accurat_hannibal.pois
WHERE Country LIKE '%B%'AND inserted_at < '2025-01-01' AND polygon IS NOT NULL AND is_active = 1 AND is_deleted = 0;

SELECT *
FROM accurat_hannibal.pois
WHERE Country LIKE '%B%'AND
	  inserted_at < '2025-01-01' AND
	  polygon IS NOT NULL AND
	  is_active = 1 AND
	  is_deleted = 0;

-- als voor elke poi er 457 (dagen) records zijn voor poi_matches
-- 497423 * 457 = 227.322.311
-- veel te veel records. ga ik nooit allemaal gebruiken.

-- 2464 * 457 = 1mil


-- dit gebruiken als filter om te zien of ze ooit inactief waren gedurende een bepaalde periode na 2025
WITH inactive_poi as (
SELECT *
FROM poi_history
WHERE (end IS NULL OR end > '2025-01-01') AND is_active = 0
),

base_filtered_poi AS (
SELECT *
FROM accurat_hannibal.pois
WHERE Country LIKE '%B%'AND
	  inserted_at < '2025-01-01' AND
      -- updated_at < '2025-01-01' AND -- geen update at in query. enkel relevant als je hieruit kan nafleiden of geom null/slecht was na 2025
	  polygon IS NOT NULL AND
	  is_active = 1 AND
      id NOT IN (SELECT poi_id FROM inactive_poi) AND
	  is_deleted = 0
)

-- nu juist nog checken op geom. net zoals inactive_poi kijken of geom ooit null was na 2025 + eventueel als geom na 2025 was aangepast eruit halen omdat poi_matches inconsistent zouden zijn + eventueel filteren op standaard vierkante geoms door te kijken of de geom maar 4 punten heeft en de afstand van linkeronderhoek tot rechterbovenhoek = afstand van rechteronderhoek tot linkerbovenhoek
-- ook joinen op poi_hours om zo openingstijden voor experimenten uit te kunnen voeren.

SELECT *
FROM base_filtered_poi
WHERE id IN (SELECT poi_id FROM poi_shape_reviews WHERE updated_at < '2025-01-01' AND status LIKE 'done')
-- GROUP BY name
-- ORDER BY count(name) DESC



-- enkel status done behouden
-- als het done is voor 2025 dan houden
-- als het done is na 2025 dan verwijderen --> updated na 2025 dus -> hoeft ook geen inserted at te doen want als een poi wordt toegevoegd, dan wordt inserted_at = updated_at
SELECT *
FROM poi_shape_reviews
WHERE updated_at < '2025-01-01' AND status LIKE 'done'

SELECT DISTINCT status
FROM poi_shape_reviews

-- 20105 als beide
-- 20331 als enkel is_active
-- 20529 enkel inactive_poi