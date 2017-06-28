using System.Collections.Generic;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DoubleReader : ColumnReader
    {
        public DoubleReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
        {
        }

        public IEnumerable<double?> Read()
        {
            var present = ReadBooleanStream(StreamKind.Present);
            var data = ReadBinaryStream(StreamKind.Data);
            var dataIndex = 0;
            if (present == null)
                while (dataIndex + 8 <= data.Length)
                {
                    var value = data.ReadDoubleBE(dataIndex);
                    dataIndex += 8;
                    yield return value;
                }
            else
                foreach (var isPresent in present)
                    if (isPresent)
                    {
                        var value = data.ReadDoubleBE(dataIndex);
                        dataIndex += 8;
                        yield return value;
                    }
                    else
                    {
                        yield return null;
                    }
        }
    }
}