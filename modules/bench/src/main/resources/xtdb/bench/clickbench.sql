-- :name q1-count :? :*
SELECT COUNT(*) FROM hits FOR ALL VALID_TIME;

-- :name q2-count-where :? :*
SELECT COUNT(*) FROM hits FOR ALL VALID_TIME WHERE adv_engine_id <> 0;

-- :name q3-sum-count-avg :? :*
SELECT SUM(adv_engine_id), COUNT(*), AVG(resolution_width) FROM hits FOR ALL VALID_TIME;

-- :name q4-avg-userid :? :*
SELECT AVG(user_id) FROM hits FOR ALL VALID_TIME;

-- :name q5-count-distinct-userid :? :*
SELECT COUNT(DISTINCT user_id) FROM hits FOR ALL VALID_TIME;

-- :name q6-count-distinct-searchphrase :? :*
SELECT COUNT(DISTINCT search_phrase) FROM hits FOR ALL VALID_TIME;

-- :name q7-min-max-eventdate :? :*
SELECT MIN(event_date), MAX(event_date) FROM hits FOR ALL VALID_TIME;

-- :name q8-advengine-count :? :*
SELECT adv_engine_id, COUNT(*) c FROM hits FOR ALL VALID_TIME WHERE adv_engine_id <> 0 GROUP BY adv_engine_id ORDER BY c DESC;

-- :name q9-region-distinct-users :? :*
SELECT region_id, COUNT(DISTINCT user_id) AS u FROM hits FOR ALL VALID_TIME GROUP BY region_id ORDER BY u DESC LIMIT 10;

-- :name q10-region-aggregates :? :*
SELECT region_id, SUM(adv_engine_id), COUNT(*) AS c, AVG(resolution_width), COUNT(DISTINCT user_id) FROM hits FOR ALL VALID_TIME GROUP BY region_id ORDER BY c DESC LIMIT 10;

-- :name q11-mobilemodel-distinct-users :? :*
SELECT mobile_phone_model, COUNT(DISTINCT user_id) AS u FROM hits FOR ALL VALID_TIME WHERE mobile_phone_model <> '' GROUP BY mobile_phone_model ORDER BY u DESC LIMIT 10;

-- :name q12-mobilephone-model-users :? :*
SELECT mobile_phone, mobile_phone_model, COUNT(DISTINCT user_id) AS u FROM hits FOR ALL VALID_TIME WHERE mobile_phone_model <> '' GROUP BY mobile_phone, mobile_phone_model ORDER BY u DESC LIMIT 10;

-- :name q13-searchphrase-count :? :*
SELECT search_phrase, COUNT(*) AS c FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' GROUP BY search_phrase ORDER BY c DESC LIMIT 10;

-- :name q14-searchphrase-distinct-users :? :*
SELECT search_phrase, COUNT(DISTINCT user_id) AS u FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' GROUP BY search_phrase ORDER BY u DESC LIMIT 10;

-- :name q15-searchengine-phrase-count :? :*
SELECT search_engine_id, search_phrase, COUNT(*) AS c FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' GROUP BY search_engine_id, search_phrase ORDER BY c DESC LIMIT 10;

-- :name q16-userid-count :? :*
SELECT user_id, COUNT(*) c FROM hits FOR ALL VALID_TIME GROUP BY user_id ORDER BY c DESC LIMIT 10;

-- :name q17-userid-phrase-count :? :*
SELECT user_id, search_phrase, COUNT(*) c FROM hits FOR ALL VALID_TIME GROUP BY user_id, search_phrase ORDER BY c DESC LIMIT 10;

-- :name q18-userid-phrase-limit :? :*
SELECT user_id, search_phrase, COUNT(*) FROM hits FOR ALL VALID_TIME GROUP BY user_id, search_phrase LIMIT 10;

-- :name q19-userid-minute-phrase :? :*
SELECT user_id, m, search_phrase, COUNT(*) AS c FROM (SELECT user_id, extract(MINUTE FROM event_time) AS m, search_phrase FROM hits FOR ALL VALID_TIME) AS sub GROUP BY user_id, m, search_phrase ORDER BY c DESC LIMIT 10;

-- :name q20-userid-lookup :? :*
SELECT user_id FROM hits FOR ALL VALID_TIME WHERE user_id = 435090932899640449;

-- :name q21-url-like-google :? :*
SELECT COUNT(*) FROM hits FOR ALL VALID_TIME WHERE url LIKE '%google%';

-- :name q22-searchphrase-url-google :? :*
SELECT search_phrase, MIN(url), COUNT(*) AS c FROM hits FOR ALL VALID_TIME WHERE url LIKE '%google%' AND search_phrase <> '' GROUP BY search_phrase ORDER BY c DESC LIMIT 10;

-- :name q23-searchphrase-title-google :? :*
SELECT search_phrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT user_id) FROM hits FOR ALL VALID_TIME WHERE title LIKE '%Google%' AND url NOT LIKE '%.google.%' AND search_phrase <> '' GROUP BY search_phrase ORDER BY c DESC LIMIT 10;

-- :name q24-star-url-google :? :*
SELECT * FROM hits FOR ALL VALID_TIME WHERE url LIKE '%google%' ORDER BY event_time LIMIT 10;

-- :name q25-searchphrase-order-eventtime :? :*
SELECT search_phrase FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' ORDER BY event_time LIMIT 10;

-- :name q26-searchphrase-order-phrase :? :*
SELECT search_phrase FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' ORDER BY search_phrase LIMIT 10;

-- :name q27-searchphrase-order-both :? :*
SELECT search_phrase FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' ORDER BY event_time, search_phrase LIMIT 10;

-- :name q28-counterid-avg-url-length :? :*
SELECT counter_id, AVG(length(url)) AS l, COUNT(*) AS c FROM hits FOR ALL VALID_TIME WHERE url <> '' GROUP BY counter_id HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

-- :name q29-referer-domain-avg-length :? :*
SELECT k, AVG(length(referer)) AS l, COUNT(*) AS c, MIN(referer) FROM (SELECT REGEXP_REPLACE(referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, referer FROM hits FOR ALL VALID_TIME WHERE referer <> '') AS sub GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

-- :name q30-sum-resolution-wide :? :*
SELECT SUM(resolution_width), SUM(resolution_width + 1), SUM(resolution_width + 2), SUM(resolution_width + 3), SUM(resolution_width + 4), SUM(resolution_width + 5), SUM(resolution_width + 6), SUM(resolution_width + 7), SUM(resolution_width + 8), SUM(resolution_width + 9), SUM(resolution_width + 10), SUM(resolution_width + 11), SUM(resolution_width + 12), SUM(resolution_width + 13), SUM(resolution_width + 14), SUM(resolution_width + 15), SUM(resolution_width + 16), SUM(resolution_width + 17), SUM(resolution_width + 18), SUM(resolution_width + 19), SUM(resolution_width + 20), SUM(resolution_width + 21), SUM(resolution_width + 22), SUM(resolution_width + 23), SUM(resolution_width + 24), SUM(resolution_width + 25), SUM(resolution_width + 26), SUM(resolution_width + 27), SUM(resolution_width + 28), SUM(resolution_width + 29), SUM(resolution_width + 30), SUM(resolution_width + 31), SUM(resolution_width + 32), SUM(resolution_width + 33), SUM(resolution_width + 34), SUM(resolution_width + 35), SUM(resolution_width + 36), SUM(resolution_width + 37), SUM(resolution_width + 38), SUM(resolution_width + 39), SUM(resolution_width + 40), SUM(resolution_width + 41), SUM(resolution_width + 42), SUM(resolution_width + 43), SUM(resolution_width + 44), SUM(resolution_width + 45), SUM(resolution_width + 46), SUM(resolution_width + 47), SUM(resolution_width + 48), SUM(resolution_width + 49), SUM(resolution_width + 50), SUM(resolution_width + 51), SUM(resolution_width + 52), SUM(resolution_width + 53), SUM(resolution_width + 54), SUM(resolution_width + 55), SUM(resolution_width + 56), SUM(resolution_width + 57), SUM(resolution_width + 58), SUM(resolution_width + 59), SUM(resolution_width + 60), SUM(resolution_width + 61), SUM(resolution_width + 62), SUM(resolution_width + 63), SUM(resolution_width + 64), SUM(resolution_width + 65), SUM(resolution_width + 66), SUM(resolution_width + 67), SUM(resolution_width + 68), SUM(resolution_width + 69), SUM(resolution_width + 70), SUM(resolution_width + 71), SUM(resolution_width + 72), SUM(resolution_width + 73), SUM(resolution_width + 74), SUM(resolution_width + 75), SUM(resolution_width + 76), SUM(resolution_width + 77), SUM(resolution_width + 78), SUM(resolution_width + 79), SUM(resolution_width + 80), SUM(resolution_width + 81), SUM(resolution_width + 82), SUM(resolution_width + 83), SUM(resolution_width + 84), SUM(resolution_width + 85), SUM(resolution_width + 86), SUM(resolution_width + 87), SUM(resolution_width + 88), SUM(resolution_width + 89) FROM hits FOR ALL VALID_TIME;

-- :name q31-searchengine-clientip-agg :? :*
SELECT search_engine_id, client_ip, COUNT(*) AS c, SUM(is_refresh), AVG(resolution_width) FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' GROUP BY search_engine_id, client_ip ORDER BY c DESC LIMIT 10;

-- :name q32-watchid-clientip-searchphrase :? :*
SELECT watch_id, client_ip, COUNT(*) AS c, SUM(is_refresh), AVG(resolution_width) FROM hits FOR ALL VALID_TIME WHERE search_phrase <> '' GROUP BY watch_id, client_ip ORDER BY c DESC LIMIT 10;

-- :name q33-watchid-clientip-all :? :*
SELECT watch_id, client_ip, COUNT(*) AS c, SUM(is_refresh), AVG(resolution_width) FROM hits FOR ALL VALID_TIME GROUP BY watch_id, client_ip ORDER BY c DESC LIMIT 10;

-- :name q34-url-count :? :*
SELECT url, COUNT(*) AS c FROM hits FOR ALL VALID_TIME GROUP BY url ORDER BY c DESC LIMIT 10;

-- :name q35-url-count-cross :? :*
SELECT one, url, COUNT(*) AS c FROM hits FOR ALL VALID_TIME, (SELECT 1) AS t (one) GROUP BY one, url ORDER BY c DESC LIMIT 10;

-- :name q36-clientip-derivations :? :*
SELECT client_ip, (client_ip - 1) AS client_ip_m1, (client_ip - 2) AS client_ip_m2, (client_ip - 3) AS client_ip_m3, COUNT(*) AS c FROM (SELECT client_ip, (client_ip - 1) AS client_ip_m1, (client_ip - 2) AS client_ip_m2, (client_ip - 3) AS client_ip_m3 FROM hits FOR ALL VALID_TIME) t GROUP BY client_ip, client_ip_m1, client_ip_m2, client_ip_m3 ORDER BY c DESC LIMIT 10;

-- :name q37-counterid-url-pageviews :? :*
SELECT url, COUNT(*) AS PageViews FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-01' AND event_date <= DATE '2013-07-31' AND dont_count_hits = 0 AND is_refresh = 0 AND url <> '' GROUP BY url ORDER BY PageViews DESC LIMIT 10;

-- :name q38-counterid-title-pageviews :? :*
SELECT title, COUNT(*) AS PageViews FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-01' AND event_date <= DATE '2013-07-31' AND dont_count_hits = 0 AND is_refresh = 0 AND title <> '' GROUP BY title ORDER BY PageViews DESC LIMIT 10;

-- :name q39-counterid-url-links :? :*
SELECT url, COUNT(*) AS PageViews FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-01' AND event_date <= DATE '2013-07-31' AND is_refresh = 0 AND is_link <> 0 AND is_download = 0 GROUP BY url ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;

-- :name q40-traficsource-pageviews :? :*
SELECT trafic_source_id, search_engine_id, adv_engine_id, Src, Dst, COUNT(*) AS PageViews FROM (SELECT trafic_source_id, search_engine_id, adv_engine_id, CASE WHEN (search_engine_id = 0 AND adv_engine_id = 0) THEN referer ELSE '' END AS Src, url AS Dst FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-01' AND event_date <= DATE '2013-07-31' AND is_refresh = 0) AS sub GROUP BY trafic_source_id, search_engine_id, adv_engine_id, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;

-- :name q41-urlhash-eventdate :? :*
SELECT url_hash, event_date, COUNT(*) AS PageViews FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-01' AND event_date <= DATE '2013-07-31' AND is_refresh = 0 AND trafic_source_id IN (-1, 6) AND referer_hash = 3594120000172545465 GROUP BY url_hash, event_date ORDER BY PageViews DESC LIMIT 10 OFFSET 100;

-- :name q42-windowclient-pageviews :? :*
SELECT window_client_width, window_client_height, COUNT(*) AS PageViews FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-01' AND event_date <= DATE '2013-07-31' AND is_refresh = 0 AND dont_count_hits = 0 AND url_hash = 2868770270353813622 GROUP BY window_client_width, window_client_height ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;

-- :name q43-minute-pageviews :? :*
SELECT M, COUNT(*) AS PageViews FROM (SELECT DATE_TRUNC(minute, event_time) AS M FROM hits FOR ALL VALID_TIME WHERE counter_id = 62 AND event_date >= DATE '2013-07-14' AND event_date <= DATE '2013-07-15' AND is_refresh = 0 AND dont_count_hits = 0) AS sub GROUP BY M ORDER BY M LIMIT 10 OFFSET 1000;
