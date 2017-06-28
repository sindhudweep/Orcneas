using System.Collections.Generic;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class FloatReader : ColumnReader
    {
        public FloatReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
        {
        }

        public IEnumerable<float?> Read()
        {
            var present = ReadBooleanStream(StreamKind.Present);
            var data = ReadBinaryStream(StreamKind.Data);
            var dataIndex = 0;
            if (present == null)
                while (dataIndex + 4 <= data.Length)
                {
                    var value = data.ReadFloatBE(dataIndex);
                    dataIndex += 4;
                    yield return value;
                }
            else
                foreach (var isPresent in present)
                    if (isPresent)
                    {
                        var value = data.ReadFloatBE(dataIndex);
                        dataIndex += 4;
                        yield return value;
                    }
                    else
                    {
                        yield return null;
                    }
        }
    }
}