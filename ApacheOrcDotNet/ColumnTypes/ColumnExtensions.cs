using System.Collections.Generic;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet.ColumnTypes
{
    public static class ColumnExtensions
    {
        public static void WriteToBuffer(this IEnumerable<IStatistics> statistics, Stream outputStream)
        {
            var indexes = new RowIndex();
            foreach (var stats in statistics)
            {
                var indexEntry = new RowIndexEntry();
                stats.FillPositionList(indexEntry.Positions);
                stats.FillColumnStatistics(indexEntry.Statistics);
                indexes.Entry.Add(indexEntry);
            }

            StaticProtoBuf.Serializer.Serialize(outputStream, indexes);
        }

        public static void AddDataStream(this StripeFooter footer, uint columnId, OrcCompressedBuffer buffer)
        {
            var stream = new Protocol.Stream
            {
                Column = columnId,
                Kind = buffer.StreamKind,
                Length = (ulong) buffer.Length
            };
            footer.Streams.Add(stream);
        }

        public static void AddColumn(this StripeFooter footer, ColumnEncodingKind columnEncodingKind,
            uint dictionarySize = 0)
        {
            var columnEncoding = new ColumnEncoding
            {
                Kind = columnEncodingKind,
                DictionarySize = dictionarySize
            };
            footer.Columns.Add(columnEncoding);
        }
    }
}