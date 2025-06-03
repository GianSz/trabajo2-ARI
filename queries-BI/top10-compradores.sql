SELECT
    customerid,
    COUNT(*) AS total_compras,
    SUM(quantity) AS cantidad_productos
FROM "AwsDataCatalog"."trabajo2db"."15"
GROUP BY customerid
ORDER BY cantidad_productos DESC
LIMIT 10;