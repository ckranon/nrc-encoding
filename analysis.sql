-- 1. Total Number of Speeches
SELECT
    COUNT(id) AS total_speeches
FROM
    speech;

-- 2. Top 10 Speakers by Speech Count
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    COUNT(s.id) AS speech_count
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
GROUP BY
    p.name, pa.name
ORDER BY
    speech_count DESC
LIMIT 10;

-- 3. Top 10 Speakers with Highest Average Anger
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.anger) AS average_anger
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_anger DESC
LIMIT 10;

-- 4. Top 10 Speakers with Lowest Average Anger
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.anger) AS average_anger
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_anger ASC
LIMIT 10;

-- 5. Top 10 Speakers with Highest Average Disgust
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.disgust) AS average_disgust
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_disgust DESC
LIMIT 10;

-- 6. Top 10 Speakers with Lowest Average Disgust
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.disgust) AS average_disgust
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_disgust ASC
LIMIT 10;

-- 7. Top 10 Speakers with Highest Average Fear
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.fear) AS average_fear
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_fear DESC
LIMIT 10;

-- 8. Top 10 Speakers with Lowest Average Fear
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.fear) AS average_fear
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_fear ASC
LIMIT 10;

-- 9. Top 10 Speakers with Highest Average Joy
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.joy) AS average_joy
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_joy DESC
LIMIT 10;

-- 10. Top 10 Speakers with Lowest Average Joy
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.joy) AS average_joy
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_joy ASC
LIMIT 10;

-- 11. Top 10 Speakers with Highest Average Sadness
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.sadness) AS average_sadness
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_sadness DESC
LIMIT 10;

-- 12. Top 10 Speakers with Lowest Average Sadness
SELECT
    p.name AS speaker_name,
    pa.name AS party_name,
    AVG(n.sadness) AS average_sadness
FROM
    speech s
JOIN
    politician p ON s.speaker = p.id
LEFT JOIN
    party pa ON p.party = pa.id
JOIN
    nrc_encoding n ON s.nrc_encoding = n.id
GROUP BY
    p.name, pa.name
ORDER BY
    average_sadness ASC
LIMIT 10;
