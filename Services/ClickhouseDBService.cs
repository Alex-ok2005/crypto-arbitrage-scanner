using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using Microsoft.Extensions.Configuration;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ArbitrageScanner.Services
{
    /// <summary>
    /// Инкапсулирует операции с ClickHouse: создание служебных таблиц, экспорт данных и выборку сделок.
    /// </summary>
    public class ClickhouseDBService : IDisposable, IDBService
    {
        private readonly ClickHouseConnection _connection;
        private readonly Logger _logger;
        private readonly BlackListsConfig _blackLists;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private bool _disposedValue;
        private record TableDefinition(string[] Fields, string Script, string? DateLocator);
        /// <summary>
        /// Хранит настройки чёрных списков для фильтрации сделок.
        /// </summary>
        private sealed class BlackListsConfig
        {
            /// <summary>
            /// Список бирж, запрещённых для покупки.
            /// </summary>
            public string[] BuyExchanges { get; init; } = Array.Empty<string>();
            /// <summary>
            /// Список бирж, запрещённых для продажи.
            /// </summary>
            public string[] SellExchanges { get; init; } = Array.Empty<string>();
            /// <summary>
            /// Список запрещённых базовых валют.
            /// </summary>
            public string[] BaseCurrency { get; init; } = Array.Empty<string>();
            /// <summary>
            /// Список запрещённых валют котирования.
            /// </summary>
            public string[] QuotedCurrency { get; init; } = Array.Empty<string>();
        }
        // Конфигурация таблиц
        private static readonly Dictionary<string, TableDefinition> TableDefinitions = new()
        {
            ["arbitrage.network_aliases"] = new(
                Fields: ["network", "canonical_network", "updated_time"],
                Script: @"CREATE TABLE IF NOT EXISTS arbitrage.network_aliases
                                (network LowCardinality(String) COMMENT 'Alias from networks.name (stored in UPPER)',
                                canonical_network LowCardinality(String) COMMENT 'Most common canonical name (UPPER)',
                                updated_time DateTime DEFAULT now())
                            ENGINE = ReplacingMergeTree(updated_time)
                            ORDER BY network
                            SETTINGS index_granularity = 8192;",
                DateLocator: null),
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
                                contract_address LowCardinality(Nullable(String)),
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
                        "buy_exchange_url", "sell_exchange_url", "selected_buy_fee", "buy_order_count", "sell_order_count", "total_buy_cost",
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
                            buy_exchange_url Nullable(String),
                            sell_exchange_url Nullable(String),
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
                        TTL event_time + toIntervalHour(1)
                        SETTINGS index_granularity = 8192;",
                DateLocator: "event_time")
        };
        private const string ProfitTradesQuery = @$"WITH
                -- Находим последние записи для каждой валюты в таблице currencies
                latest_currencies AS (
                    SELECT
                        exchange_name,
                        currency,
                        networks,
                        updated_time
                    FROM arbitrage.currencies FINAL
                    WHERE is_active is not null 
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY exchange_name, currency ORDER BY updated_time DESC) = 1
                ),

                -- Находим последние записи для каждой торговой пары и биржи в таблице trading_data
                latest_trading_data AS (
                    SELECT
                        td.exchange_name AS exchange_name,
                        td.symbol AS symbol,
                        td.base_currency AS base_currency,
                        td.quote_currency AS quote_currency,
                        upper(td.base_currency) AS base_currency_key,
                        upper(td.quote_currency) AS quote_currency_key,
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
                        AND ({{no_whitelist:Bool}} OR td.exchange_name IN {{whitelist:Array(String)}})
        	            AND date_diff('second', td.updated_time, {{now:DateTime}}) BETWEEN 0 AND {{lag_updated:Int64}}
                        AND td.symbol != 'BROCCOLI/USDT'
                ),
            
                -- Отфильтровываем записи только для спотовых рынков из таблицы market_data
                market_data_keys AS (
                    SELECT
                        md.exchange_name AS exchange_name,
                        md.base_currency AS base_currency,
                        md.quote_currency AS quote_currency,
                        upper(md.base_currency) AS base_currency_key,
                        upper(md.quote_currency) AS quote_currency_key,
                        md.type,
                        md.is_active
                    FROM arbitrage.market_data md FINAL
                ),

                spot_markets AS (
                    SELECT
                        exchange_name,
                        base_currency_key,
                        quote_currency_key
                    FROM market_data_keys
                    WHERE type = 'spot'
                ),
            
                -- Подготавливаем алиасы сетей
                network_aliases AS (
                    SELECT
                        upper(network) AS network,
                        upper(canonical_network) AS canonical_network
                    FROM arbitrage.network_aliases FINAL
                ),

                network_aliases_map AS (
                    SELECT
                        mapFromArrays(groupArray(network), groupArray(canonical_network)) AS network_map
                    FROM network_aliases
                ),
            
                -- Подготавливаем данные по общим сетям между валютами для проверки
                currency_networks AS (
                    WITH
                        (SELECT network_map FROM network_aliases_map) AS alias_map,
                        active_currency_networks AS (
                            SELECT
                                exchange_name,
                                currency,
                                upper(currency) AS currency_key,
                                network.1 AS network_name,
                                if(mapContains(alias_map, upper(network.1)),
                                    alias_map[upper(network.1)],
                                    upper(network.1)) AS canonical_network,
                                upper(network.1) AS network_upper,
                                ifNull(network.2, '') AS contract_address,
                                if(
                                    startsWith(ifNull(network.2, ''), '0x')
                                        OR startsWith(ifNull(network.2, ''), '0X'),
                                    lower(ifNull(network.2, '')),
                                    ifNull(network.2, '')
                                ) AS normalized_contract,
                                network.3 AS fee,
                                network.4 AS withdraw,
                                network.5 AS is_active
                            FROM latest_currencies
                            ARRAY JOIN networks AS network
                            WHERE network.5 = 1
                        )
                    SELECT
                        exchange_name1,
                        exchange_name2,
                        currency1,
                        currency2,
                        if(
                            currency1 = currency2,
                            currency1,
                            if(
                                max(alias_used) = 1,
                                if(length(currency1) > length(currency2),
                                    concat(currency2, ' (', currency1, ')'),
                                    concat(currency1, ' (', currency2, ')')),
                                currency1
                            )
                        ) AS currency,
                        currency1_key,
                        currency2_key,
                        groupArray(tuple(
                            if(network_upper1 != network_upper2, canonical_network1, network_name1),
                            if(contract_address1 != '', contract_address1, contract_address2),
                            buy_fee,
                            buy_withdraw,
                            buy_is_active,
                            sell_is_active
                        )) AS common_networks
                    FROM (
                        SELECT
                            c1.exchange_name AS exchange_name1,
                            c2.exchange_name AS exchange_name2,
                            c1.currency AS currency1,
                            c2.currency AS currency2,
                            c1.currency_key AS currency1_key,
                            c2.currency_key AS currency2_key,
                            c1.network_name AS network_name1,
                            c1.canonical_network AS canonical_network1,
                            c1.network_upper AS network_upper1,
                            c2.network_upper AS network_upper2,
                            c1.contract_address AS contract_address1,
                            c2.contract_address AS contract_address2,
                            c1.fee AS buy_fee,
                            c1.withdraw AS buy_withdraw,
                            c1.is_active AS buy_is_active,
                            c2.is_active AS sell_is_active,
                            if(c1.network_upper != c1.canonical_network
                                    OR c2.network_upper != c2.canonical_network, 1, 0) AS alias_used
                        FROM active_currency_networks c1
                        INNER JOIN active_currency_networks c2
                            ON c1.exchange_name != c2.exchange_name
                            AND c1.normalized_contract != ''
                            AND c1.normalized_contract = c2.normalized_contract
                            --AND c1.canonical_network = c2.canonical_network
                        --WHERE c1.normalized_contract != ''
                            --OR c2.normalized_contract != ''
                            --OR c1.normalized_contract = c2.normalized_contract
                    )
                    GROUP BY
                        exchange_name1,
                        exchange_name2,
                        currency1,
                        currency2,
                        currency1_key,
                        currency2_key
                ),
            
                -- Проверка наличия рынков типа future
                future_markets AS (
                    SELECT DISTINCT
                        exchange_name,
                        base_currency_key,
                        quote_currency_key
                    FROM market_data_keys
                    WHERE type = 'future' AND is_active = 1
                ),
            
                -- Проверка наличия рынков типа swap
                swap_markets AS (
                    SELECT DISTINCT
                        exchange_name,
                        base_currency_key,
                        quote_currency_key
                    FROM market_data_keys
                    WHERE type = 'swap' AND is_active = 1
                ),
            
               -- Основной запрос с расчётом прибыли
                profit_calculation AS (
                    SELECT
                        concat(
                            if(t1.base_currency = t2.base_currency,
                                t1.base_currency,
                                cn_base.currency),
                            '/',
                            if(t1.quote_currency = t2.quote_currency,
                                t1.quote_currency,
                                cn_quote.currency)
                        ) AS trading_pair,
                        t1.exchange_name AS buy_exchange,
                        t1.symbol AS buy_symbol,
                        t1.ask_price AS buy_price,
                        t2.exchange_name AS sell_exchange,
                        t2.symbol AS sell_symbol,
                        t2.bid_price AS sell_price,
                        least(t1.event_time, t2.event_time) AS event_time,
                        ((t2.bid_price - t1.ask_price) / t1.ask_price) * 100 AS profit_percent,
                        cn_base.common_networks AS common_networks,
                        CASE WHEN fm.exchange_name IS NOT NULL THEN 1 ELSE 0 END AS has_future_market,
                        CASE WHEN sm.exchange_name IS NOT NULL THEN 1 ELSE 0 END AS has_swap_market
                    FROM
                        latest_trading_data t1
                    JOIN latest_trading_data t2
                        ON t1.exchange_name != t2.exchange_name
                    JOIN spot_markets sm1
                        ON t1.exchange_name = sm1.exchange_name
                        AND t1.base_currency_key = sm1.base_currency_key
                        AND t1.quote_currency_key = sm1.quote_currency_key
                    JOIN spot_markets sm2
                        ON t2.exchange_name = sm2.exchange_name 
                        AND t2.base_currency_key = sm2.base_currency_key
                        AND t2.quote_currency_key = sm2.quote_currency_key
                    JOIN currency_networks cn_base
                        ON t1.exchange_name = cn_base.exchange_name1 AND t2.exchange_name = cn_base.exchange_name2
                        AND t1.base_currency_key = cn_base.currency1_key
                        AND t2.base_currency_key = cn_base.currency2_key
                    JOIN currency_networks cn_quote
                        ON t1.exchange_name = cn_quote.exchange_name1 AND t2.exchange_name = cn_quote.exchange_name2
                        AND t1.quote_currency_key = cn_quote.currency1_key
                        AND t2.quote_currency_key = cn_quote.currency2_key
                    LEFT JOIN future_markets fm
                        ON t2.exchange_name = fm.exchange_name
                        AND t2.base_currency_key = fm.base_currency_key
                        AND t2.quote_currency_key = fm.quote_currency_key
                    LEFT JOIN swap_markets sm
                        ON t2.exchange_name = sm.exchange_name
                        AND t2.base_currency_key = sm.base_currency_key
                        AND t2.quote_currency_key = sm.quote_currency_key
                    WHERE
                        t1.ask_price IS NOT NULL
                        AND t2.bid_price IS NOT NULL
                        AND t1.ask_price > 0
                        AND t2.bid_price > 0
                        AND t1.ask_price < t2.bid_price
                        AND ({{no_blacklist_buy_exchange:Bool}} OR lower(t1.exchange_name) NOT IN {{blacklist_buy_exchange:Array(String)}})
                        AND ({{no_blacklist_sell_exchange:Bool}} OR lower(t2.exchange_name) NOT IN {{blacklist_sell_exchange:Array(String)}})
                        AND ({{no_blacklist_base_currency:Bool}} OR upper(t1.base_currency) NOT IN {{blacklist_base_currency:Array(String)}})
                        AND ({{no_blacklist_quote_currency:Bool}} OR upper(t1.quote_currency) NOT IN {{blacklist_quote_currency:Array(String)}})
                        AND ((t2.bid_price - t1.ask_price) / t1.ask_price) * 100 > {{prc_min_profit:Int32}}
                        AND ((t2.bid_price - t1.ask_price) / t1.ask_price) * 100 < {{prc_max_profit:Int32}}
                )
            
            -- Выводим уникальные торговые пары с наилучшим процентом прибыли
            SELECT
                trading_pair,
                buy_exchange,
                buy_symbol,
                buy_price,
                sell_exchange,
                sell_symbol,
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
                --WHERE buy_exchange != 'gate'
                --AND sell_exchange != 'gate'
            )
            WHERE row_num = 1
            ORDER BY profit_percent DESC
            SETTINGS allow_experimental_join_condition = 1;";
        /// <summary>
        /// Инициализирует сервис ClickHouse, открывает соединение и проверяет наличие необходимых таблиц.
        /// </summary>
        /// <param name="configuration">Конфигурация приложения с параметрами подключения.</param>
        /// <param name="logger">Логгер для диагностических сообщений.</param>
        public ClickhouseDBService(IConfiguration configuration, Logger logger)
        {
            _logger = logger;
            _connection = new ClickHouseConnection(configuration.GetConnectionString("Clickhouse"));
            _blackLists = LoadBlackLists(configuration);
            // Синхронный вызов, чтобы при старте убедиться в наличии базы и таблиц
            EnsureDatabaseAndTablesAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Захватывает эксклюзивный доступ к общему соединению ClickHouse.
        /// </summary>
        /// <param name="token">Токен отмены ожидания блокировки.</param>
        /// <returns>Объект области, освобождающий блокировку при уничтожении.</returns>
        private async Task<IDisposable> EnterConnectionLockAsync(CancellationToken token = default)
        {
            await _connectionLock.WaitAsync(token);
            return new ConnectionLockScope(_connectionLock);
        }

        /// <summary>
        /// Освобождает блокировку общего соединения ClickHouse при выходе из области using.
        /// </summary>
        private sealed class ConnectionLockScope : IDisposable
        {
            private readonly SemaphoreSlim semaphore;
            private bool disposed;

            /// <summary>
            /// Создаёт область владения блокировкой соединения.
            /// </summary>
            /// <param name="semaphore">Семафор, который нужно освободить при завершении области.</param>
            public ConnectionLockScope(SemaphoreSlim semaphore)
            {
                this.semaphore = semaphore;
            }

            /// <summary>
            /// Освобождает захваченную блокировку, если она ещё не была освобождена.
            /// </summary>
            public void Dispose()
            {
                if (disposed)
                {
                    return;
                }

                semaphore.Release();
                disposed = true;
            }
        }
        /// <summary>
        /// Загружает настройки чёрных списков из конфигурации приложения.
        /// </summary>
        /// <param name="configuration">Конфигурация приложения для чтения секции BlackLists.</param>
        /// <returns>Заполненный объект конфигурации чёрных списков.</returns>
        private static BlackListsConfig LoadBlackLists(IConfiguration configuration)
        {
            var section = configuration.GetSection("BlackLists");
            return new BlackListsConfig
            {
                BuyExchanges = section.GetSection("BuyExchanges").Get<string[]>() ?? Array.Empty<string>(),
                SellExchanges = section.GetSection("SellExchanges").Get<string[]>() ?? Array.Empty<string>(),
                BaseCurrency = section.GetSection("BaseCurrency").Get<string[]>() ?? Array.Empty<string>(),
                QuotedCurrency = section.GetSection("QuotedCurrency").Get<string[]>() ?? Array.Empty<string>()
            };
        }
        /// <summary>
        /// Проверяет существование базы arbitrage и пересоздаёт все таблицы из конфигурации с последующим заполнением алиасов сетей.
        /// </summary>
        private async Task EnsureDatabaseAndTablesAsync()
        {
            try
            {
                using var cmd = _connection.CreateCommand();
                var canCreateTables = await HasCreatePermissionAsync();

                // Проверяем, существует ли база данных
                cmd.CommandText = "EXISTS DATABASE arbitrage";
                bool databaseExists = Convert.ToBoolean(await cmd.ExecuteScalarAsync());

                if (!databaseExists)
                {
                    if (!canCreateTables)
                    {
                        _logger.Warn("Нет прав на создание базы или таблиц. Пропускаем создание.");
                        return;
                    }

                    cmd.CommandText = "CREATE DATABASE IF NOT EXISTS arbitrage";
                    await cmd.ExecuteNonQueryAsync();
                    _logger.Info("Database 'arbitrage' created.");
                }

                if (!canCreateTables)
                {
                    _logger.Warn("Нет прав на пересоздание таблиц. Пропускаем создание.");
                    return;
                }

                var shouldSeedNetworkAliases = false;
                foreach (var (tableName, definition) in TableDefinitions)
                {
                    cmd.CommandText = $"DROP TABLE IF EXISTS {tableName}";
                    await cmd.ExecuteNonQueryAsync();

                    cmd.CommandText = definition.Script;
                    await cmd.ExecuteNonQueryAsync();
                    _logger.Info($"Table '{tableName}' recreated.");
                    if (tableName.Equals("arbitrage.network_aliases", StringComparison.OrdinalIgnoreCase))
                    {
                        shouldSeedNetworkAliases = true;
                    }
                }

                if (shouldSeedNetworkAliases)
                {
                    await SeedNetworkAliasesAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error while checking or creating the database/tables.");
            }
        }
        /// <summary>
        /// Проверяет, может ли текущий пользователь ClickHouse создавать и удалять временные таблицы.
        /// </summary>
        /// <returns><c>true</c>, если создание прошло успешно, иначе <c>false</c>.</returns>
        private async Task<bool> HasCreatePermissionAsync()
        {
            try
            {
                using var cmd = _connection.CreateCommand();

                // Пытаемся создать временную таблицу
                cmd.CommandText = "CREATE TEMPORARY TABLE IF NOT EXISTS __test_permissions (x UInt8)";
                await cmd.ExecuteNonQueryAsync();

                // Пытаемся удалить временную таблицу
                cmd.CommandText = "DROP TEMPORARY TABLE IF EXISTS __test_permissions";
                await cmd.ExecuteNonQueryAsync();

                return true;
            }
            catch
            {
                return false;
            }
        }
        /// <summary>
        /// Заполняет таблицу <c>arbitrage.network_aliases</c> стартовыми алиасами сетей.
        /// </summary>
        private async Task SeedNetworkAliasesAsync()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"INSERT INTO arbitrage.network_aliases (network, canonical_network) VALUES
('ARB', 'ARBITRUM'),
('ARBETH', 'ARBITRUM'),
('ARBITRUM', 'ARBITRUM'),
('ARBITRUM ONE', 'ARBITRUM'),
('ARBONE', 'ARBITRUM'),
('ARBI-NOVA', 'ARBITRUM NOVA'),
('ARBITRUM NOVA', 'ARBITRUM NOVA'),
('ARBNOVA', 'ARBITRUM NOVA'),
('AVALANCHE', 'AVAX-C'),
('AVAX', 'AVAX-C'),
('AVAX-C', 'AVAX-C'),
('AVAXC', 'AVAX-C'),
('AVAXC-CHAIN', 'AVAX-C'),
('AVAXCCHAIN', 'AVAX-C'),
('C-CHAIN', 'AVAX-C'),
('BASE', 'BASE'),
('BASE MAINNET', 'BASE'),
('BEP-20', 'BEP20'),
('BEP20', 'BEP20'),
('BINANCE SMART CHAIN', 'BEP20'),
('BNB CHAIN', 'BEP20'),
('BSC', 'BEP20'),
('BLAST', 'BLAST'),
('BLAST-ETH', 'BLAST'),
('BRC20', 'BRC20'),
('BTC_BRC20', 'BRC20'),
('BTC', 'BTC'),
('CELO', 'CELO'),
('CELO-TOKEN', 'CELO'),
('ERC-20', 'ERC20'),
('ERC20', 'ERC20'),
('ETH', 'ERC20'),
('ETHEREUM', 'ERC20'),
('FUSE', 'FUSE'),
('LINEA', 'LINEA'),
('LINEA-ETH', 'LINEA'),
('LINEAETH', 'LINEA'),
('METIS', 'METIS'),
('MODE', 'MODE'),
('MODE-ETH', 'MODE'),
('OP', 'OPTIMISM'),
('OPTIMISM', 'OPTIMISM'),
('POLYGON', 'POLYGON'),
('POLYGON BRIDGE', 'POLYGON'),
('POLYGON POS', 'POLYGON'),
('EX MATIC POLYGON', 'POLYGON'),
('MATIC', 'POLYGON'),
('RON', 'RONIN'),
('RONIN', 'RONIN'),
('SCROLL', 'SCROLL'),
('SCROLL-ETH', 'SCROLL'),
('SCROLLETH', 'SCROLL'),
('SOL', 'SOL'),
('SOLANA', 'SOL'),
('TAIKO', 'TAIKO'),
('TAIKO-ETH', 'TAIKO'),
('TAIKOETH', 'TAIKO'),
('THE OPEN NETWORK', 'TON'),
('TON', 'TON'),
('TRC10', 'TRC10'),
('TRC-20', 'TRC20'),
('TRC20', 'TRC20'),
('TRON', 'TRC20'),
('ZKSYNC', 'ZKSYNC'),
('ZKSYNC ERA', 'ZKSYNC'),
('ZKSYNCERA', 'ZKSYNC'),
('ZKSYNCERA_ETH', 'ZKSYNC');";
            await cmd.ExecuteNonQueryAsync();
        }
        /// <inheritdoc/>
        public async Task<bool> Export(DataTable data, string tableName, CancellationToken token)
        {
            if (!TableDefinitions.TryGetValue(tableName, out var tableDef))
            {
                _logger.Error($"Table {tableName} not found in definitions");
                return false;
            }

            try
            {
                using var connectionLock = await EnterConnectionLockAsync(token);
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
        /// <summary>
        /// Помечает запись как удалённую, копируя строку и устанавливая флаг удаления.
        /// </summary>
        /// <param name="trade">Сделка, по которой определяется строка для удаления.</param>
        /// <param name="tableName">Имя таблицы, в которой выполняется пометка.</param>
        /// <returns><c>true</c>, если запрос выполнен без ошибок.</returns>
        public async Task<bool> RemoveItem(ProfitTrade trade, string tableName)
        {
            if (!TableDefinitions.TryGetValue(tableName, out var tableDef))
            {
                _logger.Error($"Table {tableName} not found in definitions");
                return false;
            }

            try
            {
                using var connectionLock = await EnterConnectionLockAsync();
                using var command = _connection.CreateCommand();
                var fieldsWithoutDeletedFlag = tableDef.Fields.Where(f => f != "Is_deleted").ToList();
                var symbol = trade.Pair.GetProfitTradesSymbol();

                command.CommandText = BuildRemoveItemCommandText(tableName, tableDef.Fields, fieldsWithoutDeletedFlag);

                command.AddParameter("symbol", symbol);
                command.AddParameter("buy_exchange", trade.Pair.BuyExchange);
                command.AddParameter("sell_exchange", trade.Pair.SellExchange);

                await command.ExecuteNonQueryAsync();
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error removing item from {tableName}");
                return false;
            }
        }

        /// <summary>
        /// Формирует параметризованный SQL для пометки сделки как удалённой.
        /// </summary>
        /// <param name="tableName">Имя таблицы ClickHouse, проверенное по локальным определениям таблиц.</param>
        /// <param name="fields">Полный список полей таблицы для вставки.</param>
        /// <param name="fieldsWithoutDeletedFlag">Список полей без флага удаления.</param>
        /// <returns>Текст SQL-команды с параметрами для символа и бирж.</returns>
        private static string BuildRemoveItemCommandText(
            string tableName,
            IReadOnlyCollection<string> fields,
            IReadOnlyCollection<string> fieldsWithoutDeletedFlag)
        {
            return $@"
                    INSERT INTO {tableName} ({string.Join(", ", fields)})
                    SELECT {string.Join(", ", fieldsWithoutDeletedFlag)}, 1 AS Is_deleted
                    FROM (
                        SELECT {string.Join(", ", fieldsWithoutDeletedFlag)} 
                        FROM {tableName} FINAL 
                        WHERE symbol = {{symbol:String}}
                            AND buy_exchange = {{buy_exchange:String}}
                            AND sell_exchange = {{sell_exchange:String}}
                            AND Is_deleted = 0
                    )";
        }
        /// <inheritdoc/>
        public async Task<bool> TruncateTables()
        {
            if (TableDefinitions.Count == 0)
            {
                _logger.Error($"TableDefinitions is empty");
                return false;
            }

            try
            {
                using var connectionLock = await EnterConnectionLockAsync();
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
        /// <summary>
        /// Возвращает массив прибыльных сделок с учётом фильтров и чёрных списков из конфигурации.
        /// </summary>
        /// <param name="minVolume">Минимальный объём торговой пары.</param>
        /// <param name="prcProfitRange">Диапазон процентов прибыли для отбора сделок.</param>
        /// <param name="whitelist">Список разрешённых бирж; пустой массив означает отсутствие фильтра.</param>
        /// <param name="lagUpdated">Максимально допустимое отставание по времени обновления данных.</param>
        /// <returns>Массив прибыльных сделок.</returns>
        public async Task<ProfitTrade[]> GetProfitTrades(
            long minVolume,
            (int minValue, int maxValue) prcProfitRange,
            string[] whitelist,
            int lagUpdated)
        {
            try
            {
                var buyExchangeBlacklist = _blackLists.BuyExchanges.Select(item => item.ToLowerInvariant()).ToArray();
                var sellExchangeBlacklist = _blackLists.SellExchanges.Select(item => item.ToLowerInvariant()).ToArray();
                var baseCurrencyBlacklist = _blackLists.BaseCurrency.Select(item => item.ToUpperInvariant()).ToArray();
                var quoteCurrencyBlacklist = _blackLists.QuotedCurrency.Select(item => item.ToUpperInvariant()).ToArray();

                using var connectionLock = await EnterConnectionLockAsync();
                using var command = _connection.CreateCommand();
                command.CommandText = ProfitTradesQuery;

                command.AddParameter("min_volume", minVolume);
                command.AddParameter("prc_min_profit", prcProfitRange.minValue);
                command.AddParameter("prc_max_profit", prcProfitRange.maxValue);
                command.AddParameter("no_whitelist", whitelist.Length == 0);
                command.AddParameter("whitelist", whitelist);
                command.AddParameter("no_blacklist_buy_exchange", buyExchangeBlacklist.Length == 0);
                command.AddParameter("blacklist_buy_exchange", buyExchangeBlacklist);
                command.AddParameter("no_blacklist_sell_exchange", sellExchangeBlacklist.Length == 0);
                command.AddParameter("blacklist_sell_exchange", sellExchangeBlacklist);
                command.AddParameter("no_blacklist_base_currency", baseCurrencyBlacklist.Length == 0);
                command.AddParameter("blacklist_base_currency", baseCurrencyBlacklist);
                command.AddParameter("no_blacklist_quote_currency", quoteCurrencyBlacklist.Length == 0);
                command.AddParameter("blacklist_quote_currency", quoteCurrencyBlacklist);
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

        /// <summary>
        /// Преобразует строку результата в объект <see cref="ProfitTrade"/>.
        /// </summary>
        /// <param name="record">Строка данных с колонками прибыльной сделки.</param>
        /// <returns>Сопоставленный экземпляр прибыльной сделки.</returns>
        private static ProfitTrade MapToProfitTrade(IDataRecord record)
        {
            var networks = Array.Empty<Network>();
            if (record[9] is Tuple<string, string?, double?, byte?, byte?, byte?>[] networksWithContract)
            {
                networks = networksWithContract
                    .Select(n => new Network
                    {
                        Name = n.Item1,
                        BuyFee = n.Item3,
                        BuyWithdraw = n.Item4.HasValue ? n.Item4 != 0 : null,
                        BuyActive = n.Item5.HasValue ? n.Item5 != 0 : null,
                        SellActive = n.Item6.HasValue ? n.Item6 != 0 : null
                    }).ToArray();
            }
            else if (record[9] is Tuple<string, double?, byte?, byte?, byte?>[] networksLegacy)
            {
                networks = networksLegacy
                    .Select(n => new Network
                    {
                        Name = n.Item1,
                        BuyFee = n.Item2,
                        BuyWithdraw = n.Item3.HasValue ? n.Item3 != 0 : null,
                        BuyActive = n.Item4.HasValue ? n.Item4 != 0 : null,
                        SellActive = n.Item5.HasValue ? n.Item5 != 0 : null
                    }).ToArray();
            }

            return new ProfitTrade
            {
                Pair = new Pair
                {
                    Symbol = record.GetString(0),
                    BuyExchange = record.GetString(1),
                    BuySymbol = record.GetString(2),
                    SellExchange = record.GetString(4),
                    SellSymbol = record.GetString(5)
                },
                BuyPrice = record.GetDouble(3),
                SellPrice = record.GetDouble(6),
                EventTime = record.GetDateTime(7),
                PrcProfit = record.GetDouble(8),
                Networks = networks
            };
        }
        /// <summary>
        /// Освобождает управляемые ресурсы по запросу на Dispose.
        /// </summary>
        /// <param name="disposing">Указывает, нужно ли освобождать управляемые ресурсы.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _connection?.Dispose();
                    _connectionLock.Dispose();
                }
                _disposedValue = true;
            }
        }
        /// <summary>
        /// Освобождает ресурсы сервиса и подавляет финализацию.
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
