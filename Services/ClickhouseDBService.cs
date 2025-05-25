using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using Microsoft.Extensions.Configuration;
using NLog;
using System.Data;

namespace ArbitrageScanner.Services
{
    public class ClickhouseDBService : IDisposable, IDBService
    {
        private readonly ClickHouseConnection _connection;
        private readonly Logger _logger;
        private bool _disposedValue;
        private record TableDefinition(string[] Fields, string Script, string? DateLocator);

        // Конфигурация таблиц
        private static readonly Dictionary<string, TableDefinition> TableDefinitions = new()
        {
            ["arbitrage.trading_data"] = new(
                Fields: ["exchange_name", "symbol", "base_currency", "quote_currency", "event_time",
                        "bid_price", "ask_price", "quote_volume", "updated_time"],
                Script: @"CREATE TABLE IF NOT EXISTS arbitrage.trading_data
                            (exchange_name LowCardinality(String),
                            symbol LowCardinality(String),
                            base_currency LowCardinality(String),
                            quote_currency LowCardinality(String),
                            event_time DateTime,
                            bid_price Nullable(Float64),
                            ask_price Nullable(Float64),
                            quote_volume Nullable(Float64),
                            updated_time DateTime)
                        ENGINE = ReplacingMergeTree(event_time)
                        ORDER BY (exchange_name, symbol, event_time)
                        TTL event_time + toIntervalDay(1)
                        SETTINGS index_granularity = 8192;",
                DateLocator: null),

            ["arbitrage.market_data"] = new(
                Fields: ["exchange_name", "symbol", "base_currency", "quote_currency", "type",
                        "expiry", "expiry_datetime", "is_active", "updated_time"],
                Script: @"CREATE TABLE IF NOT EXISTS arbitrage.market_data
                            (exchange_name LowCardinality(String),
                            symbol LowCardinality(String),
                            base_currency LowCardinality(String),
                            quote_currency LowCardinality(String),
                            type LowCardinality(String),
                            expiry LowCardinality(String),
                            expiry_datetime Nullable(DateTime),
                            is_active Nullable(UInt8),
                            updated_time DateTime)
                        ENGINE = ReplacingMergeTree(updated_time)
                        ORDER BY (exchange_name, symbol, type)
                        TTL updated_time + toIntervalDay(1)
                        SETTINGS index_granularity = 8192;",
                DateLocator: null),

            ["arbitrage.currencies"] = new(
                Fields: ["exchange_name", "currency", "is_active", "networks", "updated_time"],
                Script: @"CREATE TABLE IF NOT EXISTS arbitrage.currencies
                            (exchange_name LowCardinality(String),
                            currency LowCardinality(String),
                            is_active Nullable(UInt8),
                            networks Array(Tuple(
                                name LowCardinality(String),
                                fee Nullable(Float64),
                                withdraw Nullable(UInt8),
                                is_active Nullable(UInt8))),
                            updated_time DateTime)
                        ENGINE = ReplacingMergeTree(updated_time)
                        ORDER BY (exchange_name, currency)
                        TTL updated_time + toIntervalDay(1)
                        SETTINGS index_granularity = 8192;",
                DateLocator: null),

            ["arbitrage.profit_trades"] = new(
                Fields: ["event_time", "symbol", "network_name", "buy_exchange", "sell_exchange",
                        "selected_buy_fee", "buy_order_count", "sell_order_count", "total_buy_cost",
                        "total_sell_revenue", "avg_buy_price", "avg_sell_price", "min_buy_price",
                        "max_sell_price", "min_buy_volume", "max_sell_volume", "max_profit_usdt",
                        "profit_percentage", "min_profit_usdt", "min_profit_percentage", "deposit",
                        "custom_total_buy_cost", "custom_profit_usdt", "custom_profit_percentage",
                        "Is_deleted"],
                Script: @"CREATE TABLE IF NOT EXISTS arbitrage.profit_trades
                            (event_time DateTime,
                            symbol LowCardinality(String),
                            network_name LowCardinality(String),
                            buy_exchange LowCardinality(String),
                            sell_exchange LowCardinality(String),
                            selected_buy_fee Float64,
                            buy_order_count UInt32,
                            sell_order_count UInt32,
                            total_buy_cost Float64,
                            total_sell_revenue Float64,
                            avg_buy_price Float64,
                            avg_sell_price Float64,
                            min_buy_price Float64,
                            max_sell_price Float64,
                            min_buy_volume Float64,
                            max_sell_volume Float64,
                            max_profit_usdt Float64,
                            profit_percentage Float64,
                            min_profit_usdt Float64,
                            min_profit_percentage Float64,
                            deposit Float64,
                            custom_total_buy_cost Float64,
                            custom_profit_usdt Float64,
                            custom_profit_percentage Float64,
                            Is_deleted UInt8)
                        ENGINE = ReplacingMergeTree(event_time, Is_deleted)
                        ORDER BY (symbol, buy_exchange, sell_exchange)
                        TTL event_time + toIntervalMinute(15)
                        SETTINGS index_granularity = 8192;",
                DateLocator: "event_time")
        };
        private const string ProfitTradesQuery = @$"WITH
                -- Находим последние записи для каждой торговой пары и биржи в таблице trading_data
                latest_trading_data AS (
                    SELECT
                        td.exchange_name,
                        td.symbol,
                        td.base_currency,
                        td.quote_currency,
                        td.bid_price,
                        td.ask_price,
                        td.event_time
                    FROM arbitrage.trading_data td FINAL
      	            JOIN (SELECT exchange_name, MAX(updated_time) max_updated_time
	 	            		FROM arbitrage.trading_data FINAL
    	            		GROUP BY exchange_name) ltd
    	            ON td.exchange_name = ltd.exchange_name
    	            AND td.updated_time = ltd.max_updated_time
                    WHERE bid_price is not NULL 
                    	AND ask_price is not NULL
                    	AND quote_volume > {{min_volume:Int64}}
                        AND ({{no_whitelist:Bool}} OR td.exchange_name IN({{whitelist:String}}))
        	            AND date_diff('second', {{now:DateTime}}, td.updated_time) < {{lag_updated:Int64}}
                ),
            
                -- Отфильтровываем записи только для спотовых рынков из таблицы market_data
                spot_markets AS (
                    SELECT
                        exchange_name,
                        base_currency,
                        quote_currency
                    FROM arbitrage.market_data FINAL
                    WHERE type = 'spot'
                ),
            
                -- Находим последние записи для каждой валюты в таблице currencies
                latest_currencies AS (
                    SELECT
                        exchange_name,
                        currency,
                        networks,
                        updated_time
                    FROM arbitrage.currencies FINAL
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY exchange_name, currency ORDER BY updated_time DESC) = 1
                ),
            
                -- Подготавливаем данные по общим сетям между валютами для проверки
                currency_networks AS (
                    SELECT
                        c1.exchange_name AS exchange_name1,
                        c2.exchange_name AS exchange_name2,
                        c1.currency AS currency,
                        arrayMap(
                            tpl -> tuple(
                                tpl.1 AS name,
                                arrayElement(arrayFilter(x -> x.1 = tpl.1, c1.networks), 1).2 AS buy_fee,
                                arrayElement(arrayFilter(x -> x.1 = tpl.1, c1.networks), 1).3 AS buy_withdraw,
                                arrayElement(arrayFilter(x -> x.1 = tpl.1, c1.networks), 1).4 AS buy_is_active,
                                arrayElement(arrayFilter(x -> x.1 = tpl.1, c2.networks), 1).4 AS sell_is_active
                            ),                           
                            arrayFilter(t -> indexOf(
                                    arrayIntersect(
                                    	arrayMap(tuple -> tuple.1, arrayFilter(x -> x.4 = 1 OR x.4 IS NULL, c1.networks)),
                                    	arrayMap(tuple -> tuple.1, arrayFilter(x -> x.4 = 1 OR x.4 IS NULL, c2.networks))
                                	), t.1) > 0, c1.networks)
                        ) AS common_networks
                    FROM latest_currencies c1
                    JOIN latest_currencies c2 ON c1.currency = c2.currency
                    WHERE (length(arrayIntersect(
                        arrayMap(tuple -> tuple.1, arrayFilter(x -> x.4 = 1 OR x.4 IS NULL, c1.networks)),
                        arrayMap(tuple -> tuple.1, arrayFilter(x -> x.4 = 1 OR x.4 IS NULL, c2.networks))
                    )) > 0
                    -- OR length(c1.networks) = 0
                    -- OR length(c2.networks) = 0
                    )
                    AND c1.exchange_name != c2.exchange_name
                ),
            
                -- Проверка наличия рынков типа future
                future_markets AS (
                    SELECT DISTINCT
                        exchange_name,
                        base_currency,
                        quote_currency
                    FROM arbitrage.market_data FINAL
                    WHERE type = 'future' AND is_active = 1
                ),
            
                -- Проверка наличия рынков типа swap
                swap_markets AS (
                    SELECT DISTINCT
                        exchange_name,
                        base_currency,
                        quote_currency
                    FROM arbitrage.market_data FINAL
                    WHERE type = 'swap' AND is_active = 1
                ),
            
               -- Основной запрос с расчётом прибыли
                profit_calculation AS (
                    SELECT
                        t1.symbol AS trading_pair,
                        t1.exchange_name AS buy_exchange,
                        t1.bid_price AS buy_price,
                        t2.exchange_name AS sell_exchange,
                        t2.ask_price AS sell_price,
                        least(t1.event_time, t2.event_time) AS event_time,
                        ((t2.ask_price - t1.bid_price) / t1.bid_price) * 100 AS profit_percent,
                        cn.common_networks AS common_networks,
                        CASE WHEN fm.exchange_name IS NOT NULL THEN 1 ELSE 0 END AS has_future_market,
                        CASE WHEN sm.exchange_name IS NOT NULL THEN 1 ELSE 0 END AS has_swap_market
                    FROM
                        latest_trading_data t1
                    JOIN latest_trading_data t2
                        ON t1.base_currency = t2.base_currency
                        AND t1.quote_currency = t2.quote_currency
                        AND t1.exchange_name != t2.exchange_name
                    JOIN spot_markets sm1
                        ON t1.exchange_name = sm1.exchange_name 
                        AND t1.base_currency = sm1.base_currency
                        AND t1.quote_currency = sm1.quote_currency
                    JOIN spot_markets sm2
                        ON t2.exchange_name = sm2.exchange_name 
                        AND t2.base_currency = sm2.base_currency
                        AND t2.quote_currency = sm2.quote_currency
                    JOIN currency_networks cn
                        ON t1.exchange_name = cn.exchange_name1 AND t2.exchange_name = cn.exchange_name2
                        AND t1.base_currency = cn.currency 
                    LEFT JOIN future_markets fm
                        ON t2.exchange_name = fm.exchange_name
                        AND t2.base_currency = fm.base_currency
                        AND t2.quote_currency = fm.quote_currency
                    LEFT JOIN swap_markets sm
                        ON t2.exchange_name = sm.exchange_name
                        AND t2.base_currency = sm.base_currency
                        AND t2.quote_currency = sm.quote_currency
                    WHERE
                        t1.bid_price IS NOT NULL
                        AND t2.ask_price IS NOT NULL
                        AND t1.bid_price > 0
                        AND t2.ask_price > 0
                        AND t1.bid_price < t2.ask_price
                        AND ((t2.ask_price - t1.bid_price) / t1.bid_price) * 100 > 0
                        AND ((t2.ask_price - t1.bid_price) / t1.bid_price) * 100 < 100
                )
            
            -- Выводим уникальные торговые пары с наилучшим процентом прибыли
            SELECT
                trading_pair,
                buy_exchange,
                buy_price,
                sell_exchange,
                sell_price,
                event_time,
                profit_percent,
                common_networks,
                has_future_market,
                has_swap_market
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY trading_pair ORDER BY profit_percent DESC) AS row_num
                FROM profit_calculation
                WHERE buy_exchange <> 'gate'
            )
            WHERE row_num = 1
            ORDER BY profit_percent DESC
            SETTINGS allow_experimental_join_condition = 1;";
        public ClickhouseDBService(IConfiguration configuration, Logger logger)
        {
            _logger = logger;
            _connection = new ClickHouseConnection(configuration.GetConnectionString("Clickhouse"));
            // Synchronous call to ensure the database and tables exist at startup
            EnsureDatabaseAndTablesAsync().GetAwaiter().GetResult();
        }
        private async Task EnsureDatabaseAndTablesAsync()
        {
            try
            {
                using var cmd = _connection.CreateCommand();

                // Check if the database exists
                cmd.CommandText = "EXISTS DATABASE arbitrage";
                bool databaseExists = Convert.ToBoolean(await cmd.ExecuteScalarAsync());

                if (!databaseExists)
                {
                    if (!await HasCreatePermissionAsync())
                    {
                        _logger.Warn("No permission to create the database or tables. Skipping creation.");
                        return;
                    }

                    cmd.CommandText = "CREATE DATABASE IF NOT EXISTS arbitrage";
                    await cmd.ExecuteNonQueryAsync();
                    _logger.Info("Database 'arbitrage' created.");
                }

                foreach (var (tableName, definition) in TableDefinitions)
                {
                    cmd.CommandText = $"EXISTS TABLE {tableName}";
                    var tableExists = Convert.ToBoolean(await cmd.ExecuteScalarAsync());

                    if (!tableExists)
                    {
                        if (!await HasCreatePermissionAsync())
                        {
                            _logger.Warn($"No permission to create table '{tableName}'. Skipping creation.");
                            continue;
                        }

                        cmd.CommandText = definition.Script;
                        await cmd.ExecuteNonQueryAsync();
                        _logger.Info($"Table '{tableName}' created.");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error while checking or creating the database/tables.");
            }
        }
        private async Task<bool> HasCreatePermissionAsync()
        {
            try
            {
                using var cmd = _connection.CreateCommand();

                // Attempt to create a temporary table
                cmd.CommandText = "CREATE TEMPORARY TABLE IF NOT EXISTS __test_permissions (x UInt8)";
                await cmd.ExecuteNonQueryAsync();

                // Attempt to drop the temporary table
                cmd.CommandText = "DROP TEMPORARY TABLE IF EXISTS __test_permissions";
                await cmd.ExecuteNonQueryAsync();

                return true;
            }
            catch
            {
                return false;
            }
        }
        public async Task<bool> Export(DataTable data, string tableName, CancellationToken token)
        {
            if (!TableDefinitions.TryGetValue(tableName, out var tableDef))
            {
                _logger.Error($"Table {tableName} not found in definitions");
                return false;
            }

            try
            {
                using var bulkCopy = new ClickHouseBulkCopy(_connection)
                {
                    DestinationTableName = tableName,
                    ColumnNames = tableDef.Fields,
                    BatchSize = 1000000
                };

                await bulkCopy.InitAsync();
                await bulkCopy.WriteToServerAsync(data, token);
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error exporting to {tableName}");
                return false;
            }
        }
        public async Task<bool> RemoveItem(ProfitTrade trade, string tableName)
        {
            if (!TableDefinitions.TryGetValue(tableName, out var tableDef))
            {
                _logger.Error($"Table {tableName} not found in definitions");
                return false;
            }

            try
            {
                using var command = _connection.CreateCommand();
                var fieldsWithoutDeletedFlag = tableDef.Fields.Where(f => f != "Is_deleted").ToList();

                command.CommandText = $@"
                    INSERT INTO {tableName} ({string.Join(", ", tableDef.Fields)})
                    SELECT {string.Join(", ", fieldsWithoutDeletedFlag)}, 1 AS Is_deleted
                    FROM (
                        SELECT {string.Join(", ", fieldsWithoutDeletedFlag)} 
                        FROM {tableName} FINAL 
                        WHERE symbol = '{trade.Pair.Symbol}' 
                            AND buy_exchange = '{trade.Pair.BuyExchange}' 
                            AND sell_exchange = '{trade.Pair.SellExchange}')";

                await command.ExecuteNonQueryAsync();
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error removing item from {tableName}");
                return false;
            }
        }
        public async Task<bool> TruncateTables()
        {
            if (TableDefinitions.Count == 0)
            {
                _logger.Error($"TableDefinitions is empty");
                return false;
            }

            try
            {
                foreach (var tableDef in TableDefinitions)
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = $"TRUNCATE TABLE {tableDef.Key};";
                    await command.ExecuteNonQueryAsync();
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error truncate tables");
                return false;
            }
        }
        public async Task<ProfitTrade[]> GetProfitTrades(
            long minVolume,
            (int minValue, int maxValue) prcProfitRange,
            string[] whitelist,
            int lagUpdated)
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = ProfitTradesQuery;

                command.AddParameter("min_volume", minVolume);
                command.AddParameter("prc_min_profit", prcProfitRange.minValue);
                command.AddParameter("prc_max_profit", prcProfitRange.maxValue);
                command.AddParameter("no_whitelist", whitelist.Length == 0);
                command.AddParameter("whitelist", whitelist.Length > 0 ? $"'{string.Join("','", whitelist)}'" : "''");
                command.AddParameter("now", DateTime.Now);
                command.AddParameter("lag_updated", lagUpdated);

                using var reader = await command.ExecuteReaderAsync();

                var result = new List<ProfitTrade>();
                while (await reader.ReadAsync())
                {
                    result.Add(MapToProfitTrade(reader));
                }

                _logger.Info($"Retrieved {result.Count} profitable trades from the database.");
                return result.ToArray();
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error retrieving profit trades.");
                return [];
            }
        }
        private static ProfitTrade MapToProfitTrade(IDataRecord record)
        {
            return new ProfitTrade
            {
                Pair = new Pair
                {
                    Symbol = record.GetString(0),
                    BuyExchange = record.GetString(1),
                    SellExchange = record.GetString(3)
                },
                BuyPrice = record.GetDouble(2),
                SellPrice = record.GetDouble(4),
                EventTime = record.GetDateTime(5),
                PrcProfit = record.GetDouble(6),
                Networks = ((record[7] as Tuple<string, double?, byte?, byte?, byte?>[]) ?? [])
                    .Select(n => new Network
                    {
                        Name = n.Item1,
                        BuyFee = n.Item2,
                        BuyWithdraw = n.Item3.HasValue ? n.Item3 != 0 : null,
                        BuyActive = n.Item4.HasValue ? n.Item4 != 0 : null,
                        SellActive = n.Item5.HasValue ? n.Item5 != 0 : null
                    }).ToArray()
            };
        }
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _connection?.Dispose();
                }
                _disposedValue = true;
            }
        }
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
