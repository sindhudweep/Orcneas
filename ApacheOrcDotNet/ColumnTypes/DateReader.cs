using System;
using System.Collections.Generic;
using System.IO;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DateReader : ColumnReader
    {
        private static readonly DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
        {
        }

        public IEnumerable<DateTime?> Read()
        {
            var present = ReadBooleanStream(StreamKind.Present);
            var data = ReadNumericStream(StreamKind.Data, true);
            if (present == null)
            {
                foreach (var value in data)
                    yield return _unixEpoch.AddTicks(value * TimeSpan.TicksPerDay);
            }
            else
            {
                var valueEnumerator = ((IEnumerable<long>) data).GetEnumerator();
                foreach (var isPresent in present)
                    if (isPresent)
                    {
                        var success = valueEnumerator.MoveNext();
                        if (!success)
                            throw new InvalidDataException(
                                "The PRESENT data stream's length didn't match the DATA stream's length");
                        yield return _unixEpoch.AddTicks(valueEnumerator.Current * TimeSpan.TicksPerDay);
                    }
                    else
                    {
                        yield return null;
                    }
            }
        }
    }
}