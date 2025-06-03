%flink.ssql

DROP TABLE IF EXISTS input_purchases;
CREATE TABLE input_purchases (
    InvoiceNo string, 
    StockCode string, 
    Description string, 
    Quantity bigint, 
    InvoiceDate string, 
    UnitPrice double, 
    Customer bigint, 
    Country string,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'kinesis',
    'stream' = 'acmecoOrders',
    'aws.region' = 'us-east-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
);

DROP TABLE IF EXISTS low_stock_alerts;
CREATE TABLE low_stock_alerts (
    stockcode string,
    total_quantity bigint
) WITH (
    'connector' = 'kinesis',
    'stream' = 'acmecoOrders-Output',
    'aws.region' = 'us-east-1',
    'format' = 'json'
);

INSERT INTO low_stock_alerts
SELECT 
    StockCode AS stockcode,
    SUM(Quantity) AS total_quantity
FROM input_purchases
GROUP BY
    StockCode,
    TUMBLE(event_time, INTERVAL '5' SECOND)
HAVING SUM(Quantity) > 50;