using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace ArbitrageScanner.Models
{
    /// <summary>
    /// Потокобезопасная очередь торговых символов биржи с дедупликацией ожидающих обработки элементов.
    /// </summary>
    public sealed class ExchangeSymbolQueue
    {
        private readonly ConcurrentQueue<string> queue = new();
        private readonly ConcurrentDictionary<string, byte> queuedSymbols = new(StringComparer.Ordinal);

        /// <summary>
        /// Добавляет символ в очередь, если он ещё не ожидает обработки.
        /// </summary>
        /// <param name="symbol">Торговый символ, который нужно поставить в очередь.</param>
        /// <returns><c>true</c>, если символ был добавлен; иначе <c>false</c>.</returns>
        public bool EnqueueIfMissing(string? symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol))
            {
                return false;
            }

            var normalizedSymbol = symbol.Trim();
            if (!queuedSymbols.TryAdd(normalizedSymbol, 0))
            {
                return false;
            }

            queue.Enqueue(normalizedSymbol);
            return true;
        }

        /// <summary>
        /// Извлекает следующий символ из очереди и освобождает его для повторной постановки.
        /// </summary>
        /// <param name="symbol">Извлечённый торговый символ или <c>null</c>, если очередь пуста.</param>
        /// <returns><c>true</c>, если символ был извлечён; иначе <c>false</c>.</returns>
        public bool TryDequeue([NotNullWhen(true)] out string? symbol)
        {
            while (queue.TryDequeue(out var dequeuedSymbol))
            {
                queuedSymbols.TryRemove(dequeuedSymbol, out _);
                symbol = dequeuedSymbol;
                return true;
            }

            symbol = null;
            return false;
        }
    }
}
