using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using ccxt;
using Microsoft.Extensions.Configuration;
//using ccxt.pro;
using NLog;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ArbitrageScanner.Services
{
    /// <summary>
    /// Управляет подключениями к биржам, сбором рыночных данных и фоновыми запросами стаканов.
    /// </summary>
    public class ExchangeService : IExchangeService, IDisposable
    {
        private const int DefaultRecvWindowMs = 20000;
        private static readonly TimeSpan TimeDifferenceRefreshInterval = TimeSpan.FromHours(1);
        private readonly Logger logger;
        private readonly Random random = new();
        private readonly CancellationTokenSource cts = new();
        private readonly ConcurrentDictionary<string, bool> timeDifferenceUnsupported = new();
        /// <summary>
        /// Событие, уведомляющее подписчиков об обновлённых стаканах.
        /// </summary>
        public event putOrderBookHandler? EventOrderBook;

        // Список бирж с комиссиями тейкера, объектами бирж и очередями символов
        // Значение "taker fee" зависит от вашего тарифного плана на бирже
        // Внимание! Не все биржи отдают данные без регистрации и получения API-ключей.
        // Ключи задаются в секции ExchangeCredentials конфигурации (см. ApplyCredentials), а не в коде.
        public Dictionary<string, (decimal fee, Exchange exchange, ExchangeSymbolQueue symbolQueue, ExchangeSpotUrlSettings spotUrlSettings)> Exchanges { get; } = new()
        {
            { "kraken", (0.0026m, new Kraken(), new(),
                new ExchangeSpotUrlSettings(
                    "https://pro.kraken.com/app/trade/{BASE}-{QUOTE}",
                    "Верхний регистр, разделитель '-'.",
                    "https://www.kraken.com/ru-ru")) },
            //{ "binance", (0.001m, new binance(), new(),
            //    new ExchangeSpotUrlSettings(
            //        "https://www.binance.com/ru/trade/{BASE}_{QUOTE}?type=spot",
            //        "Верхний регистр, разделитель '_'.",
            //        "https://www.binance.com/ru")) },
            { "okx", (0.001m, new okx(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.okx.com/ru/trade-spot/{BASE_LOWER}-{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '-'.",
                    "https://www.okx.com/ru")) },
            { "gate", (0.002m, new gate(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.gate.io/ru/trade/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://www.gate.io/ru")) },
            { "bybit", (0.001m, new bybit(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.bybit.com/ru-RU/trade/spot/{BASE}/{QUOTE}",
                    "Верхний регистр, разделитель '/'.",
                    "https://www.bybit.com/ru-RU/")) },
            { "bitmex", (0.001m, new bitmex(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.bitmex.com/app/trade/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://www.bitmex.com/")) },
            { "bitget", (0.001m, new bitget(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.bitget.com/ru/spot/{BASE}{QUOTE}",
                    "Верхний регистр, без разделителя.",
                    "https://www.bitget.com/ru/")) },
            { "xt", (0.002m, new xt(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.xt.com/ru/trade/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://www.xt.com/ru")) },
            { "mexc", (0.002m, new mexc(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.mexc.com/exchange/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://www.mexc.com/")) },
            { "bitfinex", (0.001m, new bitfinex(), new(),
                new ExchangeSpotUrlSettings(
                    "https://trading.bitfinex.com/t/{BASE}:{QUOTE}",
                    "Верхний регистр, разделитель ':'.",
                    "https://www.bitfinex.com/")) },
            { "bitmart", (0.001m, new bitmart(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.bitmart.com/trade/ru?symbol={BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_' в параметре symbol.",
                    "https://www.bitmart.com/ru")) },
            { "coinbase", (0.001m, new coinbase(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.coinbase.com/advanced-trade/spot/{BASE}-{QUOTE}",
                    "Верхний регистр, разделитель '-'.",
                    "https://www.coinbase.com/")) },
            { "coinex", (0.001m, new coinex(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.coinex.com/exchange/{BASE_LOWER}-{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '-'.",
                    "https://www.coinex.com/")) },
            { "hashkey", (0.001m, new hashkey(), new(),
                new ExchangeSpotUrlSettings(
                    "https://global.hashkey.com/en-US/spot/{BASE}_{QUOTE}",
                    "Нижний регистр, разделитель '_'.",
                    "https://global.hashkey.com/")) },
            { "hyperliquid", (0.001m, new hyperliquid(), new(),
                new ExchangeSpotUrlSettings(
                    "https://app.hyperliquid.xyz/trade/{BASE}/{QUOTE}",
                    "Верхний регистр, разделитель '-'.",
                    "https://app.hyperliquid.xyz/")) },
            { "htx", (0.001m, new htx(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.htx.com/ru-ru/trade/{BASE_LOWER}_{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '_'.",
                    "https://www.htx.com/ru-ru/")) },
            { "kucoin", (0.001m, new kucoin(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.kucoin.com/ru/trade/{BASE}-{QUOTE}",
                    "Верхний регистр, разделитель '-'.",
                    "https://www.kucoin.com/ru"))
            },
            { "whitebit", (0.001m, new whitebit(), new(),
                new ExchangeSpotUrlSettings(
                    "https://whitebit.com/ru/trade/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://whitebit.com/ru")) },
            { "exmo", (0.001m, new exmo(), new(),
                new ExchangeSpotUrlSettings(
                    "https://exmo.me/ru/trade/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://exmo.me/ru")) },
            { "poloniex",(0.002m, new poloniex(), new(),
                new ExchangeSpotUrlSettings(
                    "https://poloniex.com/trade/{BASE}_{QUOTE}",
                    "Верхний регистр, разделитель '_'.",
                    "https://poloniex.com/")) },

            //{ "cryptocom",(0.002m, new cryptocom(), new()) },
            { "phemex",(0.001m, new phemex(), new(),
                new ExchangeSpotUrlSettings(
                    "https://phemex.com/spot/trade/{BASE}{QUOTE}",
                    "Верхний регистр, без разделителя.",
                    "https://phemex.com/")) },
            { "hollaex",(0.001m, new hollaex(), new(),
                new ExchangeSpotUrlSettings(
                    null,
                    "Типовой страницы спота нет, использовать сайт.",
                    "https://hollaex.com/")) },
            //{ "timex",(0.001m, new timex(), new(),
            //    new ExchangeSpotUrlSettings(
            //        "https://timex.io/private/trading/{BASE}{QUOTE}",
            //        "Верхний регистр, без разделителя.",
            //        "https://timex.io/ru")) },
            { "bitopro",(0.001m, new bitopro(), new(),
                new ExchangeSpotUrlSettings(
                    null,
                    "Типовой страницы спота нет, использовать сайт.",
                    "https://www.bitopro.com/")) },
            { "bitrue",(0.001m, new bitrue(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.bitrue.com/trade/{BASE_LOWER}_{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '_'.",
                    "https://www.bitrue.com/")) },

            //{ "bingx",(0.001m, new bingx(), new()) },
            //{ "ascendex",(0.001m, new ascendex(), new()) },
            { "bequant",(0.001m, new bequant(), new(),
                new ExchangeSpotUrlSettings(
                    "https://bequant.io/{BASE_LOWER}-to-{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '-to-'.",
                    "https://bequant.io/")) },
            { "bigone",(0.001m, new bigone(), new(),
                new ExchangeSpotUrlSettings(
                    "https://big.one/trade/{BASE}-{QUOTE}",
                    "Верхний регистр, разделитель '-'.",
                    "https://big.one/")) },
            //{ "bit2c",(0.001m, new bit2c(), new()) },
            //{ "bitbank",(0.001m, new bitbank(), new()) },
            { "lbank",(0.001m, new lbank(), new(),
                new ExchangeSpotUrlSettings(
                    "https://www.lbank.com/trade/{BASE_LOWER}_{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '_'.",
                    "https://www.lbank.com/")) },
            { "bitbns",(0.001m, new bitbns(), new(),
                new ExchangeSpotUrlSettings(
                    "https://bitbns.com/trade/#/{BASE_LOWER}-{QUOTE_LOWER}",
                    "Нижний регистр, разделитель '-'.",
                    "https://bitbns.com/")) }
        };
        /// <summary>
        /// Инициализирует сервис бирж с логгером, применяет API-ключи из конфигурации и запускает фоновые задачи для каждой биржи.
        /// </summary>
        /// <param name="_logger">Логгер для диагностики.</param>
        /// <param name="configuration">Конфигурация приложения с необязательной секцией ExchangeCredentials.</param>
        public ExchangeService(Logger _logger, IConfiguration configuration)
        {
            logger = _logger;
            ApplyCredentials(configuration);
            Init();
        }
        /// <summary>
        /// Применяет API-ключи бирж из секции ExchangeCredentials:{exchangeId} (ApiKey, Secret, Password).
        /// Значения можно задать в user secrets или переменными окружения, например ExchangeCredentials__okx__ApiKey.
        /// </summary>
        /// <param name="configuration">Конфигурация приложения.</param>
        private void ApplyCredentials(IConfiguration configuration)
        {
            foreach (var (exchangeId, exchangeData) in Exchanges)
            {
                var section = configuration.GetSection($"ExchangeCredentials:{exchangeId}");
                var apiKey = section["ApiKey"];
                var secret = section["Secret"];
                if (string.IsNullOrWhiteSpace(apiKey) || string.IsNullOrWhiteSpace(secret))
                    continue;

                exchangeData.exchange.apiKey = apiKey;
                exchangeData.exchange.secret = secret;
                var password = section["Password"];
                if (!string.IsNullOrWhiteSpace(password))
                    exchangeData.exchange.password = password;

                logger.Info($"API credentials applied for exchange {exchangeId}.");
            }
        }
        /// <summary>
        /// Запускает задачи опроса для каждой настроенной биржи.
        /// </summary>
        private void Init()
        {
            foreach (var exchangeId in Exchanges.Keys)
            {
                if (Exchanges.TryGetValue(exchangeId, out var exchangeData))
                {
                    ApplyTimeSyncOptions(exchangeData.exchange);
                    StartTimeDifferenceSyncTask(exchangeId, exchangeData.exchange, cts.Token);
                }
                StartExchangeTask(exchangeId, cts.Token);
            }
        }
        /// <summary>
        /// Применяет настройки синхронизации времени к объекту биржи ccxt.
        /// </summary>
        /// <param name="exchange">Экземпляр биржи для настройки.</param>
        private static void ApplyTimeSyncOptions(Exchange exchange)
        {
            if (exchange.options is not ConcurrentDictionary<string, object> options)
            {
                options = new ConcurrentDictionary<string, object>();
                exchange.options = options;
            }

            options["adjustForTimeDifference"] = true;
            options["recvWindow"] = DefaultRecvWindowMs;
        }
        /// <summary>
        /// Запускает периодическую синхронизацию временной разницы с биржей.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для логирования.</param>
        /// <param name="exchange">Экземпляр биржи ccxt для синхронизации.</param>
        /// <param name="token">Токен отмены для остановки задачи.</param>
        private void StartTimeDifferenceSyncTask(string exchangeId, Exchange exchange, CancellationToken token)
        {
            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await LoadTimeDifferenceSafe(exchangeId, exchange);
                    try
                    {
                        await Task.Delay(TimeDifferenceRefreshInterval, token);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                }
            }, token);
        }
        /// <summary>
        /// Безопасно загружает временную разницу между локальным временем и временем биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для логирования.</param>
        /// <param name="exchange">Экземпляр биржи ccxt.</param>
        private async Task LoadTimeDifferenceSafe(string exchangeId, Exchange exchange)
        {
            try
            {
                if (!SupportsTimeDifference(exchange))
                {
                    if (timeDifferenceUnsupported.TryAdd(exchangeId, true))
                    {
                        logger.Info($"Биржа {exchangeId} не поддерживает синхронизацию времени через ccxt.");
                    }
                    return;
                }

                await exchange.loadTimeDifference();
            }
            catch (Exception ex)
            {
                logger.Warn(ex, $"Не удалось обновить временную разницу для биржи {exchangeId}.");
            }
        }
        /// <summary>
        /// Проверяет, доступна ли синхронизация времени для биржи через ccxt.
        /// </summary>
        /// <param name="exchange">Экземпляр биржи ccxt для проверки.</param>
        /// <returns>True, если биржа поддерживает запрос времени.</returns>
        private static bool SupportsTimeDifference(Exchange exchange)
        {
            if (exchange.has is IDictionary<string, object> capabilities
                && capabilities.TryGetValue("fetchTime", out var fetchTimeValue))
            {
                return fetchTimeValue is true;
            }

            return false;
        }
        /// <summary>
        /// Запускает фонового работника, который извлекает символы из очереди и запрашивает стаканы.
        /// </summary>
        /// <param name="exchangeId">Идентификатор опрашиваемой биржи.</param>
        /// <param name="token">Токен отмены для остановки работника.</param>
        private void StartExchangeTask(string exchangeId, CancellationToken token)
        {
            // Запускаем задачу для опроса очереди
            Task.Run(async () =>
            {
                if (Exchanges.TryGetValue(exchangeId, out var exchangeData))
                {
                    var (fee, exchange, queue, _) = exchangeData;
                    if (exchange != null && queue != null)
                        while (!token.IsCancellationRequested)
                        {
                            try
                            {
                                if (queue.TryDequeue(out string? symbol))
                                {
                                    var orderBook = await GetOrderBook(exchangeId, symbol);
                                    if (orderBook != null)
                                        EventOrderBook?.Invoke(exchangeId, orderBook, token);
                                }
                                else
                                    // Добавляем задержку перед следующим опросом очереди
                                    await Task.Delay(random.Next(200, 501));
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex, $"Error in thread for exchange {exchangeId}");
                            }
                        }
                }
            }, token);
        }
        /// <summary>
        /// Получает определения рынков с указанной биржи и подготавливает их для сохранения.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <returns>Таблица данных с метаданными рынков и заготовками отпечатков.</returns>
        public async Task<DataTable> GetMarketData(string exchangeId)
        {
            var data = new DataTable();
            data.Columns.AddRange(
            [
                new DataColumn("exchange_name", typeof(string)),
                new DataColumn("symbol", typeof(string)),
                new DataColumn("base_currency", typeof(string)),
                new DataColumn("quote_currency", typeof(string)),
                new DataColumn("type", typeof(string)),
                new DataColumn("expiry", typeof(string)),
                new DataColumn("expiry_datetime", typeof(DateTime)),
                new DataColumn("is_active", typeof(bool)),
                new DataColumn("updated_time", typeof(DateTime))
            ]);

            try
            {
                // Получаем данные с биржи
                DateTime dateTime = DateTime.Now;
                var markets = await Exchanges[exchangeId].exchange.loadMarkets();

                if (markets is Dictionary<string, object> dict)
                {
                    foreach (var item in dict)
                    {
                        if (item.Value is Dictionary<string, object> itemDict)
                        {
                            bool? is_active = null;
                            if (itemDict["active"] is not null)
                            {
                                if (itemDict["active"].ToString().ToLower() == "true")
                                    is_active = true;
                                else if (itemDict["active"].ToString().ToLower() == "false")
                                    is_active = false;
                            }

                            data.Rows.Add(
                            [
                                exchangeId,                     // exchangeId - идентификатор биржи
                                itemDict["symbol"],             // symbol - торговая пара (например, BTC/USDT)
                                itemDict["base"],               // base_currency - базовая валюта (например, BTC)
                                itemDict["quote"],              // quote_currency - валюта котировки (например, USDT)
                                itemDict["type"],
                                itemDict["expiry"],
                                itemDict["expiryDatetime"],
                                is_active,
                                dateTime,                       // updated_time
                            ]);
                        }
                    }
                }

                logger.Info($"{data.Rows.Count} pairs loaded for exchange {Exchanges[exchangeId].exchange.name}.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetMarketsData', exchange {exchangeId}");
            }
            return data;
        }
        /// <summary>
        /// Загружает срезы тикеров для биржи и дополняет их заготовками метаданных отпечатков.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <returns>Таблица данных с ценами тикеров и метаданными.</returns>
        public async Task<DataTable> GetTickers(string exchangeId)
        {
            var data = new DataTable();
            data.Columns.AddRange(
            [
                new DataColumn("exchange_name", typeof(string)),
                new DataColumn("symbol", typeof(string)),
                new DataColumn("base_currency", typeof(string)),
                new DataColumn("quote_currency", typeof(string)),
                new DataColumn("event_time", typeof(DateTime)),
                new DataColumn("bid_price", typeof(double)),
                new DataColumn("ask_price", typeof(double)),
                new DataColumn("quote_volume", typeof(double)),
                new DataColumn("updated_time", typeof(DateTime))
            ]);

            try
            {
                int emptyData = 0;
                DateTime eventTime = DateTime.Now;
                DateTime updatedTime = eventTime;
                var tickers = await Exchanges[exchangeId].exchange.FetchTickers();
                if (tickers.tickers is Dictionary<string, Ticker> _tickers)
                {
                    foreach (var _ticker in _tickers)
                    {
                        // Подготавливаем данные для вставки
                        var currencies = _ticker.Key.Split("/");
                        if (currencies.Length < 2)
                        {
                            emptyData++;
                            continue;
                        }

                        // Если дата не указана — используем текущую
                        if (DateTime.TryParse(_ticker.Value.datetime, out DateTime _datetime))
                        {
                            eventTime = _datetime;
                        }

                        data.Rows.Add(
                        [
                            exchangeId,                     // exchangeId - идентификатор биржи
                            _ticker.Key,                    // symbol - торговая пара (например, BTC/USDT)
                            currencies[0],                  // base_currency - базовая валюта (например, BTC)
                            currencies[1],                  // quote_currency - валюта котировки (например, USDT)
                            eventTime,                      // event_time
                            _ticker.Value.bid,              // bid_price - цена покупки (ask)
                            _ticker.Value.ask,              // ask_price - цена продажи (bid)
                            _ticker.Value.quoteVolume,      // quoteVolume
                            updatedTime                     // updated_time
                        ]);
                    }
                }
                logger.Info($"{data.Rows.Count} pairs loaded for exchange {Exchanges[exchangeId].exchange.name}.");
                logger.Info($"{emptyData} empty rows excluded.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetTickers', exchange {exchangeId}");
            }
            return data;
        }
        /// <summary>
        /// Получает метаданные валют для заданной биржи, включая сведения о сетях и контрактах.
        /// Для KuCoin использует локально загруженное поле currencies, а для остальных бирж — FetchCurrencies.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <returns>Таблица данных валют для экспорта.</returns>
        public async Task<DataTable> GetCurrencies(string exchangeId)
        {
            var data = new DataTable();
            data.Columns.AddRange([
                    new DataColumn("exchange_name", typeof(string)),
                    new DataColumn("currency", typeof(string)),
                    new DataColumn("is_active", typeof(bool)),
                    new DataColumn("networks", typeof((string, string?, double?, bool?, bool?)[])),
                    new DataColumn("updated_time", typeof(DateTime))
                ]);
            try
            {
                int emptyData = 0;
                DateTime dateTime = DateTime.Now;

                object? currencies;

                if (exchangeId == "kucoin")
                {
                    await Exchanges[exchangeId].exchange.LoadMarkets();
                    currencies = Exchanges[exchangeId].exchange.currencies;
                }
                else
                {
                    var _cur = await Exchanges[exchangeId].exchange.FetchCurrencies();
                    currencies = _cur.currencies;
                }

                foreach (var currency in EnumerateCurrencies(currencies))
                {
                    data.Rows.Add([
                            exchangeId,
                            currency.Code,
                            currency.Active,
                            currency.Networks != null ?
                                currency.Networks.Select(p =>
                                    (p.Key, TryExtractContract(p.Value.info), p.Value.fee, p.Value.withdraw, p.Value.active)).ToArray() : [],
                            dateTime
                        ]);
                }

                logger.Info($"{data.Rows.Count} currencies loaded for exchange {Exchanges[exchangeId].exchange.name}.");
                logger.Info($"{emptyData} empty rows excluded.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetCurrencies', exchange {exchangeId}");
            }
            return data;
        }
        /// <summary>
        /// Загружает стакан для указанного торгового символа с выбранной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <param name="symbol">Торговый символ, для которого нужен стакан.</param>
        /// <returns>Разобранный стакан или <c>null</c> в случае неудачи.</returns>
        public async Task<VerifyOrderBook?> GetOrderBook(string exchangeId, string symbol)
        {
            try
            {
                DateTime dateTime = DateTime.Now;
                var orderBook =  await Exchanges[exchangeId].exchange.FetchOrderBook(symbol);
                return new VerifyOrderBook()
                {
                    ExchangeName = exchangeId,
                    Symbol = orderBook.symbol,
                    Asks = orderBook.asks.Where(order => order.Count > 1)
                              .Select(order => (price: order[0], volume: order[1])).ToArray(),
                    Bids = orderBook.bids.Where(order => order.Count > 1)
                              .Select(order => (price: order[0], volume: order[1])).ToArray(),
                    UpdatedTime = orderBook.datetime is null ?  dateTime : Convert.ToDateTime(orderBook.datetime)
                };
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error in method 'GetOrderBook', exchange {exchangeId}");
                return null;
            }
        }
        /// <summary>
        /// Нормализует набор валют, возвращая код, активность и список сетей вне зависимости от формата данных.
        /// </summary>
        /// <param name="currencies">Сырой объект валют из FetchCurrencies или exchange.currencies.</param>
        /// <returns>Коллекция данных валют, пригодная для построения таблицы.</returns>
        private static IEnumerable<(string? Code, bool? Active, Dictionary<string, ccxt.Network>? Networks)> EnumerateCurrencies(object? currencies)
        {
            if (currencies is Dictionary<string, Currency> typedCurrencies)
            {
                foreach (var currency in typedCurrencies)
                {
                    yield return (currency.Value.code ?? currency.Key, currency.Value.active, currency.Value.networks);
                }
                yield break;
            }

            if (currencies is IDictionary currenciesDictionary)
            {
                foreach (DictionaryEntry entry in currenciesDictionary)
                {
                    if (entry.Value is Currency currency)
                    {
                        yield return (currency.code ?? entry.Key?.ToString(), currency.active, currency.networks);
                        continue;
                    }

                    if (entry.Value is IDictionary<string, object> rawCurrency)
                    {
                        rawCurrency.Remove("fees");
                        rawCurrency.Add("margin", null);
                        if (rawCurrency.TryGetValue("limits", out var limitsValue) && limitsValue is null)
                        {
                            rawCurrency.Remove("limits");
                        }
                        if (!rawCurrency.TryGetValue("networks", out var networksValue) || networksValue is null)
                        {
                            rawCurrency["networks"] = new Dictionary<string, object>();
                        }
                        else if (networksValue is IDictionary nonGenericNetworks)
                        {
                            if (nonGenericNetworks.Count == 0)
                            {
                                rawCurrency["networks"] = new Dictionary<string, object>();
                            }
                        }
                        else if (networksValue is not IDictionary<string, object>)
                        {
                            rawCurrency["networks"] = new Dictionary<string, object>();
                        }
                        var parsedCurrency = new Currency(rawCurrency);
                        yield return (parsedCurrency.code ?? entry.Key?.ToString(), parsedCurrency.active, parsedCurrency.networks);
                    }
                }
            }
        }
        /// <summary>
        /// Извлекает адрес контракта из словаря info, учитывая приоритет ключей, фильтрацию ложных ключей и валидацию значений.
        /// </summary>
        /// <param name="info">Сырой словарь info из ccxt.</param>
        /// <returns>Строку адреса контракта или null, если корректный адрес не найден.</returns>
        private static string? TryExtractContract(object? info)
        {
            if (info is not IDictionary infoDict)
            {
                return null;
            }

            var candidateKeys = new[]
            {
                "contractaddress",
                "contract_address",
                "ctaddr",
                "tokenaddress",
                "contract",
                "addr",
                "asset_id.contract_address"
            };

            var candidates = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            string? explorerAssetUrl = null;

            foreach (DictionaryEntry entry in infoDict)
            {
                if (entry.Key is not string key)
                {
                    continue;
                }

                var lowerKey = key.ToLowerInvariant();
                if (lowerKey == "explorer_asset_url" && entry.Value is string urlValue)
                {
                    explorerAssetUrl = urlValue;
                }

                if (!IsContractKeyAllowed(lowerKey))
                {
                    continue;
                }

                if (!candidates.ContainsKey(lowerKey))
                {
                    candidates[lowerKey] = entry.Value;
                }

                if (lowerKey == "asset_id" && entry.Value is IDictionary assetInfo)
                {
                    foreach (DictionaryEntry assetEntry in assetInfo)
                    {
                        if (assetEntry.Key is not string assetKey)
                        {
                            continue;
                        }

                        if (assetKey.Equals("contract_address", StringComparison.OrdinalIgnoreCase))
                        {
                            candidates["asset_id.contract_address"] = assetEntry.Value;
                            break;
                        }
                    }
                }
            }

            foreach (var candidateKey in candidateKeys)
            {
                if (!candidates.TryGetValue(candidateKey, out var candidateValue))
                {
                    continue;
                }

                var normalized = NormalizeContractValue(candidateValue);
                if (normalized is not null)
                {
                    return normalized;
                }
            }

            return TryExtractTokenIdFromExplorerUrl(explorerAssetUrl);
        }
        /// <summary>
        /// Проверяет, подходит ли имя ключа для извлечения адреса контракта.
        /// </summary>
        /// <param name="lowerKey">Имя ключа в нижнем регистре.</param>
        /// <returns>True, если ключ можно использовать, иначе false.</returns>
        private static bool IsContractKeyAllowed(string lowerKey)
        {
            if (lowerKey.Contains("regex") || lowerKey.EndsWith("_regex", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return !lowerKey.Contains("depositaddress")
                   && !lowerKey.Contains("deposit_address")
                   && !lowerKey.Contains("memo")
                   && !lowerKey.Contains("tag")
                   && !lowerKey.Contains("paymentid")
                   && !lowerKey.Contains("explorer")
                   && !lowerKey.Contains("browserurl");
        }
        /// <summary>
        /// Нормализует и валидирует значение адреса контракта.
        /// </summary>
        /// <param name="value">Сырое значение из словаря info.</param>
        /// <returns>Очищенную строку адреса или null, если значение некорректно.</returns>
        private static string? NormalizeContractValue(object? value)
        {
            if (value is not string stringValue)
            {
                return null;
            }

            var trimmed = stringValue.Trim();
            if (string.IsNullOrWhiteSpace(trimmed))
            {
                return null;
            }

            if (trimmed.Contains("System.Collections.Generic", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            if (trimmed.IndexOfAny(new[] { '^', '$', '{', '}', '[', ']' }) >= 0)
            {
                return null;
            }

            if (trimmed.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
                && (uri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase)
                    || uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase)))
            {
                return null;
            }

            return trimmed;
        }
        /// <summary>
        /// Пытается извлечь адрес контракта из URL обозревателя токена по шаблонам /token/ или /token20/.
        /// </summary>
        /// <param name="explorerAssetUrl">URL обозревателя токена.</param>
        /// <returns>Извлечённый адрес токена или null, если извлечь не удалось.</returns>
        private static string? TryExtractTokenIdFromExplorerUrl(string? explorerAssetUrl)
        {
            if (string.IsNullOrWhiteSpace(explorerAssetUrl))
            {
                return null;
            }

            var trimmedUrl = explorerAssetUrl.Trim();
            var path = trimmedUrl;

            if (Uri.TryCreate(trimmedUrl, UriKind.Absolute, out var uri))
            {
                path = uri.AbsolutePath;
            }

            foreach (var marker in new[] { "/token20/", "/token/" })
            {
                var index = path.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
                if (index < 0)
                {
                    continue;
                }

                var startIndex = index + marker.Length;
                if (startIndex >= path.Length)
                {
                    continue;
                }

                var remaining = path.Substring(startIndex);
                var endIndex = remaining.IndexOf('/');
                var tokenId = endIndex >= 0 ? remaining.Substring(0, endIndex) : remaining;
                var normalized = NormalizeContractValue(tokenId);
                if (normalized is not null)
                {
                    return normalized;
                }
            }

            return null;
        }
        /// <summary>
        /// Освобождает ресурсы, используемые сервисом, и отменяет фоновые задачи.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        /// <summary>
        /// Выполняет непосредственную очистку, при необходимости освобождая управляемые ресурсы.
        /// </summary>
        /// <param name="disposing">True, если нужно освободить управляемые объекты вместе с неуправляемыми.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                cts.Cancel();
            }
        }
    }
}
