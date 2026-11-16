using System;
using System.Collections.Generic;

namespace ArbitrageScanner.Models
{
    public struct TokenFingerprint : IEquatable<TokenFingerprint>
    {
        public string? ChainId { get; init; }
        public string? Contract { get; init; }
        public int? Decimals { get; init; }

        /// <summary>
        /// Вычисляет хеш-код отпечатка, используя нормализованные поля.
        /// </summary>
        /// <returns>Детерминированное значение хеша.</returns>
        public override int GetHashCode()
        {
            return HashCode.Combine(ChainId?.ToLowerInvariant(), Contract?.ToLowerInvariant(), Decimals);
        }

        /// <summary>
        /// Определяет равенство двух отпечатков, сравнивая строки без учёта регистра.
        /// </summary>
        /// <param name="other">Отпечаток для сравнения.</param>
        /// <returns><c>true</c>, если совпадают сеть, контракт и число знаков.</returns>
        public bool Equals(TokenFingerprint other)
        {
            return string.Equals(ChainId, other.ChainId, StringComparison.OrdinalIgnoreCase)
                && string.Equals(Contract, other.Contract, StringComparison.OrdinalIgnoreCase)
                && Decimals == other.Decimals;
        }

        public override bool Equals(object obj) => obj is TokenFingerprint other && Equals(other);

        /// <summary>
        /// Проверяет, что отпечаток не содержит идентифицирующих данных.
        /// </summary>
        /// <returns><c>true</c>, если сеть, контракт и decimals отсутствуют.</returns>
        public bool IsEmpty()
        {
            return string.IsNullOrWhiteSpace(ChainId)
                && string.IsNullOrWhiteSpace(Contract)
                && !Decimals.HasValue;
        }

        /// <summary>
        /// Формирует компактное строковое представление отпечатка, укорачивая контракт при необходимости.
        /// </summary>
        /// <returns>Читаемое описание отпечатка.</returns>
        public override string ToString()
        {
            if (IsEmpty())
            {
                return "unknown";
            }

            var chain = string.IsNullOrWhiteSpace(ChainId) ? "?" : ChainId;
            var contract = string.IsNullOrWhiteSpace(Contract)
                ? "?"
                : Contract.Length > 10
                    ? $"{Contract[..6]}…{Contract[^4..]}"
                    : Contract;
            var decimalsText = Decimals.HasValue ? Decimals.Value.ToString() : "?";

            return $"{chain} {contract} ({decimalsText}d)";
        }
    }

    public class MarketDescriptor
    {
        public TokenFingerprint? Base { get; init; }
        public TokenFingerprint? Quote { get; init; }
        public string Symbol { get; init; }
        public string ExchangeId { get; init; }

        /// <summary>
        /// Формирует читаемое представление дескриптора рынка для диагностики.
        /// </summary>
        /// <returns>Строка с символом, отпечатками и идентификатором биржи.</returns>
        public string ToDisplayString()
        {
            var baseText = Base?.ToString() ?? "unknown";
            var quoteText = Quote?.ToString() ?? "unknown";
            return $"{Symbol} [{baseText}] / [{quoteText}] @ {ExchangeId}";
        }
    }

    // Состояние сделок
    public enum ProfitTradesState
    {
        New = 0,            // Новая сделка
        Checking = 1,       // Сделка проверяется
        Checked = 2,        // Проверена
        Unconfirmed = 3,    // Неподтверждена    
        ReChecking = 4,     // Перепроверка
        Expired = 5         // Истекла
    }
    public struct PairStruct : IEquatable<PairStruct>
    {
        public string Symbol { get; }
        public string BuyExchange { get; }
        public string SellExchange { get; }
        public TokenFingerprint? Base { get; }
        public TokenFingerprint? Quote { get; }

        public PairStruct(
            string symbol,
            string buyExchange,
            string sellExchange,
            TokenFingerprint? baseFingerprint = null,
            TokenFingerprint? quoteFingerprint = null)
        {
            Symbol = symbol;
            BuyExchange = buyExchange;
            SellExchange = sellExchange;
            Base = baseFingerprint;
            Quote = quoteFingerprint;
        }
        public override bool Equals(object obj) => obj is PairStruct other && Equals(other);
        /// <summary>
        /// Сравнивает две структуры пары с учётом отпечатков, если они указаны.
        /// </summary>
        /// <param name="other">Структура пары для сравнения.</param>
        /// <returns><c>true</c>, когда совпадают биржи, символ и при наличии отпечатки.</returns>
        public bool Equals(PairStruct other)
        {
            var hasFingerprint = Base.HasValue || Quote.HasValue || other.Base.HasValue || other.Quote.HasValue;

            if (hasFingerprint)
            {
                return BuyExchange == other.BuyExchange
                    && SellExchange == other.SellExchange
                    && Nullable.Equals(Base, other.Base)
                    && Nullable.Equals(Quote, other.Quote)
                    && Symbol == other.Symbol;
            }

            return Symbol == other.Symbol &&
                   BuyExchange == other.BuyExchange &&
                   SellExchange == other.SellExchange;
        }
        /// <summary>
        /// Вычисляет хеш-код, включающий символ, биржи и отпечатки.
        /// </summary>
        /// <returns>Хеш-код, представляющий пару.</returns>
        public override int GetHashCode()
        {
            return HashCode.Combine(Symbol, BuyExchange, SellExchange, Base, Quote);
        }
        /// <summary>
        /// Возвращает читаемое описание направления пары.
        /// </summary>
        /// <returns>Текстовое представление пары.</returns>
        public override string ToString() => $"{Symbol} ({BuyExchange} → {SellExchange})";
    }
    /// <summary>
    /// Описывает торговую пару с привязкой к биржам и их обозначениям.
    /// </summary>
    public class Pair
    {
        public string Symbol { get; set; } // Торговая пара
        public string BuyExchange { get; set; } // Биржа покупки
        public string SellExchange { get; set; } // Биржа продажи
        public string BuySymbol { get; set; } // Символ биржи покупки
        public string SellSymbol { get; set; } // Символ биржи продажи
        public TokenFingerprint? Base { get; set; }
        public TokenFingerprint? Quote { get; set; }
        /// <summary>
        /// Формирует человекочитаемое название пары с учётом отпечатков, если они есть.
        /// </summary>
        /// <returns>Отформатированное имя пары.</returns>
        public string GetName()
        {
            var baseText = Base?.ToString();
            var quoteText = Quote?.ToString();
            var fingerprintText = (baseText != null || quoteText != null)
                ? $" | base: {baseText ?? "unknown"}, quote: {quoteText ?? "unknown"}"
                : string.Empty;

            return $"{Symbol}: {BuyExchange} => {SellExchange}{fingerprintText}";
        }

        /// <summary>
        /// Формирует символ пары для записи в таблицу profit_trades с учётом различий базовых валют.
        /// </summary>
        /// <returns>Символ в формате BASE/QUOTE или BASE1(BASE2)/QUOTE в верхнем регистре.</returns>
        public string GetProfitTradesSymbol()
        {
            var buyParts = SplitSymbolParts(BuySymbol);
            var sellParts = SplitSymbolParts(SellSymbol);
            var primaryParts = SplitSymbolParts(Symbol);

            var baseBuy = buyParts.BaseSymbol ?? primaryParts.BaseSymbol;
            var baseSell = sellParts.BaseSymbol ?? primaryParts.BaseSymbol;
            var quoteSymbol = buyParts.QuoteSymbol ?? sellParts.QuoteSymbol ?? primaryParts.QuoteSymbol;

            if (string.IsNullOrWhiteSpace(baseBuy) && string.IsNullOrWhiteSpace(baseSell))
            {
                return (Symbol ?? string.Empty).ToUpperInvariant();
            }

            var baseSymbol = string.IsNullOrWhiteSpace(baseBuy) ? baseSell : baseBuy;

            if (!string.IsNullOrWhiteSpace(baseBuy)
                && !string.IsNullOrWhiteSpace(baseSell)
                && !string.Equals(baseBuy, baseSell, StringComparison.OrdinalIgnoreCase))
            {
                var first = baseBuy;
                var second = baseSell;

                if (baseSell.Length < baseBuy.Length)
                {
                    (first, second) = (baseSell, baseBuy);
                }

                var baseText = $"{first.ToUpperInvariant()}({second.ToUpperInvariant()})";
                return $"{baseText}/{(quoteSymbol ?? string.Empty).ToUpperInvariant()}";
            }

            if (string.IsNullOrWhiteSpace(quoteSymbol))
            {
                return baseSymbol?.ToUpperInvariant() ?? (Symbol ?? string.Empty).ToUpperInvariant();
            }

            return $"{baseSymbol?.ToUpperInvariant()}/{quoteSymbol.ToUpperInvariant()}";
        }

        /// <summary>
        /// Возвращает базовую и котируемую валюты для указанной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи, для которой нужно подобрать символы.</param>
        /// <returns>Кортеж с базовой и котируемой валютой или пустыми значениями при неудаче.</returns>
        public (string? BaseSymbol, string? QuoteSymbol) GetSymbolsForExchange(string exchangeId)
        {
            var candidates = new List<string?>();

            if (string.Equals(exchangeId, BuyExchange, StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add(BuySymbol);
                candidates.Add(Symbol);
                candidates.Add(SellSymbol);
            }
            else if (string.Equals(exchangeId, SellExchange, StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add(SellSymbol);
                candidates.Add(Symbol);
                candidates.Add(BuySymbol);
            }
            else
            {
                candidates.Add(Symbol);
                candidates.Add(BuySymbol);
                candidates.Add(SellSymbol);
            }

            foreach (var candidate in candidates)
            {
                var parts = SplitSymbolParts(candidate);
                if (!string.IsNullOrWhiteSpace(parts.BaseSymbol) && !string.IsNullOrWhiteSpace(parts.QuoteSymbol))
                {
                    return parts;
                }
            }

            return (null, null);
        }

        /// <summary>
        /// Разбирает символ торговой пары на базовую и котируемую валюты.
        /// </summary>
        /// <param name="symbol">Символ торговой пары, содержащий разделитель.</param>
        /// <returns>Кортеж с базовой и котируемой валютой либо пустыми значениями.</returns>
        private static (string? BaseSymbol, string? QuoteSymbol) SplitSymbolParts(string? symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol))
            {
                return (null, null);
            }

            foreach (var separator in new[] { '/', '-', '_' })
            {
                var parts = symbol.Split(separator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (parts.Length == 2)
                {
                    return (parts[0], parts[1]);
                }
            }

            return (null, null);
        }
    }
    public class ProfitTrade
    {
        public Pair Pair { get; set; } // Торговая пара
        public double BuyPrice { get; set; } // Цена покупки
        public double SellPrice { get; set; } // Цена продажи
        public DateTime EventTime { get; set; } // Дата события
        public double PrcProfit { get; set; } // Процент профита
        public Network[] Networks { get; set; } = []; // Общие сети бирж
        public bool? HasFuture { get; set; } // Наличие рынка фьючерсов для торговой пары
        public bool? HasSwap { get; set; } // Наличие рынка бессрочных фьючерсов для торговой пары
        public double PrcRealProfit { get; set; } // Процент профита после проверки
        public ProfitTradesState State { get; set; } = ProfitTradesState.New; // Состояние
        public DateTime? VerifyTime { get; set; } // Дата начала проверки
        public VerifyOrderBook? BuyOrderBook { get; set; }
        public VerifyOrderBook? SellOrderBook { get; set; }
        /// <summary>
        /// Создаёт неглубокую копию сделки, сохраняя пару и метаданные отпечатков.
        /// </summary>
        /// <returns>Новый экземпляр <see cref="ProfitTrade"/>, повторяющий текущие значения.</returns>
        public ProfitTrade Clone() => new()
        {
            Pair = new()
            {
                Symbol = Pair.Symbol,
                BuyExchange = Pair.BuyExchange,
                SellExchange = Pair.SellExchange,
                BuySymbol = Pair.BuySymbol,
                SellSymbol = Pair.SellSymbol,
                Base = Pair.Base,
                Quote = Pair.Quote
            },
            BuyPrice = BuyPrice,
            SellPrice = SellPrice,
            EventTime = EventTime,
            PrcProfit = PrcProfit,
            Networks = [.. Networks],
            HasFuture = HasFuture,
            HasSwap = HasSwap,
            State = State,
            VerifyTime = VerifyTime,
            BuyOrderBook = BuyOrderBook,
            SellOrderBook = SellOrderBook
        };
    }
    public class Network
    {
        public string Name { get; set; } // Название сети
        public double? BuyFee { get; set; } // Комиссия сети на бирже покупки
        public bool? BuyWithdraw { get; set; } // Возможность вывода с биржи покупки
        public bool? BuyActive { get; set; } // Активность сети на бирже покупки
        public bool? SellActive { get; set; } // Активность сети на бирже продажи
    }
    public class VerifyOrderBook
    {
        public string ExchangeName { get; set; } // Идентификатор биржи
        public string Symbol { get; set; } // Торговый символ
        public TokenFingerprint? Base { get; set; }
        public TokenFingerprint? Quote { get; set; }
        public (double price, double volume)[] Asks { get; set; } = [];// Агрегированные заявки на продажу
        public (double price, double volume)[] Bids { get; set; } = [];// Агрегированные заявки на покупку
        public DateTime UpdatedTime { get; set; } // Время обновления стакана
    }

    public class SpotSpotDeal
    {
        public DateTime EventTime { get; set; }
        public string Symbol { get; set; }
        public TokenFingerprint? Base { get; set; }
        public TokenFingerprint? Quote { get; set; }
        public string NetworkName { get; set; }
        public string BuyExchange { get; set; }
        public string SellExchange { get; set; }
        public string? BuyExchangeUrl { get; set; }
        public string? SellExchangeUrl { get; set; }
        public double SelectedBuyFee { get; set; }
        public int BuyOrderCount { get; set; }
        public int SellOrderCount { get; set; }
        public double TotalBuyCost { get; set; }
        public double TotalSellRevenue { get; set; }
        public double AvgBuyPrice { get; set; }
        public double AvgSellPrice { get; set; }
        public double MinBuyPrice { get; set; }
        public double MaxSellPrice { get; set; }
        public double MinBuyVolume { get; set; }
        public double MaxSellVolume { get; set; }
        public double MaxProfitUSDT { get; set; }
        public double ProfitPercentage { get; set; }
        public double MinProfitUSDT { get; set; }
        public double MinProfitPercentage { get; set; }
        public double Deposit { get; set; }
        public double CustomTotalBuyCost { get; set; }
        public double CustomProfitUSDT { get; set; }
        public double CustomProfitPercentage { get; set; }
    }
}
