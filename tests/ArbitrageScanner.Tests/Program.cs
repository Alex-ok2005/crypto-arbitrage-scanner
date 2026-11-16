using ArbitrageScanner.Models;
using ArbitrageScanner.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace ArbitrageScanner.Tests
{
    /// <summary>
    /// Запускает набор регрессионных тестов для исправлений очередей, SQL-расчёта и удаления сделок.
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Точка входа тестового приложения, выполняющая все проверки и возвращающая код ошибки при сбое.
        /// </summary>
        /// <param name="args">Аргументы командной строки, не используемые тестовым раннером.</param>
        /// <returns>Код завершения процесса: 0 при успехе, 1 при провале тестов.</returns>
        private static async Task<int> Main(string[] args)
        {
            var tests = new (string Name, Func<Task> Run)[]
            {
                ("ExchangeSymbolQueue дедуплицирует ожидающий символ", ExchangeSymbolQueueDeduplicatesPendingSymbol),
                ("ExchangeSymbolQueue безопасно принимает конкурентные добавления", ExchangeSymbolQueueHandlesConcurrentEnqueue),
                ("ProfitTradesQuery использует ask для покупки, bid для продажи и корректный лаг", ProfitTradesQueryUsesExecutableSpreadPricesAndFreshnessWindow),
                ("RemoveItem SQL использует параметры и фильтр активной записи", RemoveItemCommandUsesParametersAndDeletedFilter)
            };

            var failed = 0;
            foreach (var test in tests)
            {
                try
                {
                    await test.Run();
                    Console.WriteLine($"PASS: {test.Name}");
                }
                catch (Exception ex)
                {
                    failed++;
                    Console.Error.WriteLine($"FAIL: {test.Name}");
                    Console.Error.WriteLine(ex.Message);
                }
            }

            if (failed == 0)
            {
                Console.WriteLine($"All tests passed: {tests.Length}");
                return 0;
            }

            Console.Error.WriteLine($"Tests failed: {failed} of {tests.Length}");
            return 1;
        }

        /// <summary>
        /// Проверяет, что очередь не принимает повторный символ до его извлечения и снова принимает после извлечения.
        /// </summary>
        private static Task ExchangeSymbolQueueDeduplicatesPendingSymbol()
        {
            var queue = new ExchangeSymbolQueue();

            AssertTrue(queue.EnqueueIfMissing("BTC/USDT"), "Первое добавление символа должно пройти.");
            AssertFalse(queue.EnqueueIfMissing("BTC/USDT"), "Повторный ожидающий символ не должен добавляться.");
            AssertTrue(queue.TryDequeue(out var firstSymbol), "Символ должен извлекаться из очереди.");
            AssertEqual("BTC/USDT", firstSymbol, "Очередь должна вернуть исходный символ.");
            AssertTrue(queue.EnqueueIfMissing("BTC/USDT"), "После извлечения символ можно поставить в очередь повторно.");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Проверяет атомарную дедупликацию при одновременной постановке одинакового символа из многих задач.
        /// </summary>
        private static async Task ExchangeSymbolQueueHandlesConcurrentEnqueue()
        {
            var queue = new ExchangeSymbolQueue();
            var tasks = Enumerable.Range(0, 64)
                .Select(_ => Task.Run(() => queue.EnqueueIfMissing("ETH/USDT")))
                .ToArray();

            var results = await Task.WhenAll(tasks);
            AssertEqual(1, results.Count(result => result), "Только одна конкурентная постановка одинакового символа должна быть успешной.");
            AssertTrue(queue.TryDequeue(out var symbol), "Единственный добавленный символ должен извлекаться.");
            AssertEqual("ETH/USDT", symbol, "Извлечённый символ должен соответствовать добавленному.");
            AssertFalse(queue.TryDequeue(out _), "Повторных одинаковых символов в очереди быть не должно.");
        }

        /// <summary>
        /// Проверяет, что запрос прибыльных сделок считает спред по исполнимым ценам и корректно фильтрует свежесть данных.
        /// </summary>
        private static Task ProfitTradesQueryUsesExecutableSpreadPricesAndFreshnessWindow()
        {
            var query = GetPrivateConstant<string>(typeof(ClickhouseDBService), "ProfitTradesQuery");

            AssertContains(query, "date_diff('second', td.updated_time, {now:DateTime}) BETWEEN 0 AND {lag_updated:Int64}", "Фильтр свежести должен считать возраст записи от updated_time к now.");
            AssertContains(query, "t1.ask_price AS buy_price", "Цена покупки должна браться из ask.");
            AssertContains(query, "t2.bid_price AS sell_price", "Цена продажи должна браться из bid.");
            AssertContains(query, "((t2.bid_price - t1.ask_price) / t1.ask_price) * 100 AS profit_percent", "Процент прибыли должен использовать исполнимый спред.");
            AssertDoesNotContain(query, "t1.bid_price AS buy_price", "Цена покупки не должна браться из bid.");
            AssertDoesNotContain(query, "t2.ask_price AS sell_price", "Цена продажи не должна браться из ask.");
            AssertDoesNotContain(query, "date_diff('second', {now:DateTime}, td.updated_time)", "Фильтр свежести не должен считать дату в обратном направлении.");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Проверяет, что SQL удаления сделки строится с параметрами и не включает значения сделки напрямую.
        /// </summary>
        private static Task RemoveItemCommandUsesParametersAndDeletedFilter()
        {
            var commandText = InvokePrivateStatic<string>(
                typeof(ClickhouseDBService),
                "BuildRemoveItemCommandText",
                "arbitrage.profit_trades",
                new[] { "event_time", "symbol", "buy_exchange", "sell_exchange", "Is_deleted" },
                new[] { "event_time", "symbol", "buy_exchange", "sell_exchange" });

            AssertContains(commandText, "WHERE symbol = {symbol:String}", "Символ должен передаваться параметром.");
            AssertContains(commandText, "AND buy_exchange = {buy_exchange:String}", "Биржа покупки должна передаваться параметром.");
            AssertContains(commandText, "AND sell_exchange = {sell_exchange:String}", "Биржа продажи должна передаваться параметром.");
            AssertContains(commandText, "AND Is_deleted = 0", "Удаление должно копировать только активную запись.");
            AssertDoesNotContain(commandText, "WHERE symbol = '", "SQL не должен интерполировать символ как строковый литерал.");
            AssertDoesNotContain(commandText, "AND buy_exchange = '", "SQL не должен интерполировать биржу покупки как строковый литерал.");
            AssertDoesNotContain(commandText, "AND sell_exchange = '", "SQL не должен интерполировать биржу продажи как строковый литерал.");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Возвращает значение приватной константы через reflection.
        /// </summary>
        /// <typeparam name="T">Ожидаемый тип значения константы.</typeparam>
        /// <param name="declaringType">Тип, в котором объявлена константа.</param>
        /// <param name="fieldName">Имя приватного поля-константы.</param>
        /// <returns>Значение константы указанного типа.</returns>
        private static T GetPrivateConstant<T>(Type declaringType, string fieldName)
        {
            var field = declaringType.GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException($"Поле '{fieldName}' не найдено.");
            return (T)(field.GetRawConstantValue()
                ?? throw new InvalidOperationException($"Поле '{fieldName}' не содержит значения."));
        }

        /// <summary>
        /// Вызывает приватный статический метод через reflection.
        /// </summary>
        /// <typeparam name="T">Ожидаемый тип результата метода.</typeparam>
        /// <param name="declaringType">Тип, в котором объявлен метод.</param>
        /// <param name="methodName">Имя приватного статического метода.</param>
        /// <param name="arguments">Аргументы вызова метода.</param>
        /// <returns>Результат вызова метода указанного типа.</returns>
        private static T InvokePrivateStatic<T>(Type declaringType, string methodName, params object[] arguments)
        {
            var method = declaringType.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException($"Метод '{methodName}' не найден.");
            return (T)(method.Invoke(null, arguments)
                ?? throw new InvalidOperationException($"Метод '{methodName}' вернул null."));
        }

        /// <summary>
        /// Проверяет истинность условия и выбрасывает исключение при нарушении.
        /// </summary>
        /// <param name="condition">Проверяемое условие.</param>
        /// <param name="message">Сообщение об ошибке.</param>
        private static void AssertTrue(bool condition, string message)
        {
            if (!condition)
            {
                throw new InvalidOperationException(message);
            }
        }

        /// <summary>
        /// Проверяет ложность условия и выбрасывает исключение при нарушении.
        /// </summary>
        /// <param name="condition">Проверяемое условие.</param>
        /// <param name="message">Сообщение об ошибке.</param>
        private static void AssertFalse(bool condition, string message)
        {
            if (condition)
            {
                throw new InvalidOperationException(message);
            }
        }

        /// <summary>
        /// Проверяет равенство ожидаемого и фактического значений.
        /// </summary>
        /// <typeparam name="T">Тип сравниваемых значений.</typeparam>
        /// <param name="expected">Ожидаемое значение.</param>
        /// <param name="actual">Фактическое значение.</param>
        /// <param name="message">Сообщение об ошибке.</param>
        private static void AssertEqual<T>(T expected, T actual, string message)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
            {
                throw new InvalidOperationException($"{message} Ожидалось: {expected}; получено: {actual}.");
            }
        }

        /// <summary>
        /// Проверяет наличие фрагмента в строке.
        /// </summary>
        /// <param name="text">Строка для проверки.</param>
        /// <param name="expectedFragment">Ожидаемый фрагмент.</param>
        /// <param name="message">Сообщение об ошибке.</param>
        private static void AssertContains(string text, string expectedFragment, string message)
        {
            if (!text.Contains(expectedFragment, StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"{message} Не найден фрагмент: {expectedFragment}");
            }
        }

        /// <summary>
        /// Проверяет отсутствие фрагмента в строке.
        /// </summary>
        /// <param name="text">Строка для проверки.</param>
        /// <param name="unexpectedFragment">Недопустимый фрагмент.</param>
        /// <param name="message">Сообщение об ошибке.</param>
        private static void AssertDoesNotContain(string text, string unexpectedFragment, string message)
        {
            if (text.Contains(unexpectedFragment, StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"{message} Найден недопустимый фрагмент: {unexpectedFragment}");
            }
        }
    }
}
