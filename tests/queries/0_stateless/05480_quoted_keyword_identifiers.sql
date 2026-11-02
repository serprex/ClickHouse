-- An identifier stays quoted exactly when it spells a keyword the parser would misread, in any case.
-- Prefixes, extensions and names with underscores or digits around a keyword stay bare.
SELECT w, formatQuerySingleLine('SELECT `' || w || '`')
FROM
(
    SELECT arrayJoin(arrayFlatten(arrayMap(
        k -> [k, upper(k), initcap(k), k || 'x', substring(k, 1, length(k) - 1), '_' || k, k || '_', k || '1'],
        ['inf', 'infinity', 'nan', 'true', 'false', 'distinct', 'all', 'some',
         'table', 'select', 'cube', 'rollup', 'recursive', 'from', 'top', 'values']))) AS w
)
ORDER BY w;
