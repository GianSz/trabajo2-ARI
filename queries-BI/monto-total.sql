SELECT
    COUNT(*) AS total_compras,
    SUM(unitprice) AS monto_total
FROM "AWSDataCatalog"."trabajo2db"."15";