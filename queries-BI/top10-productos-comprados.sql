SELECT
    description,
    SUM(quantity) AS cantidad_compras
FROM "AwsDataCatalog"."trabajo2db"."15"
GROUP BY description
ORDER BY cantidad_compras DESC
LIMIT 10;