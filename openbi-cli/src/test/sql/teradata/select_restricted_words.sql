SELECT
    restricted_word
FROM
    SYSLIB.SQLRestrictedWords
WHERE
    category = 'R'
ORDER BY
    restricted_word