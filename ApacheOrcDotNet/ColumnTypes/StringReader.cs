using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class StringReader : ColumnReader
    {
        public StringReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
        {
        }

        public IEnumerable<string> Read()
        {
            var kind = GetColumnEncodingKind(StreamKind.Data);
            switch (kind)
            {
                case ColumnEncodingKind.DirectV2: return ReadDirectV2();
                case ColumnEncodingKind.DictionaryV2: return ReadDictionaryV2();
                default: throw new NotImplementedException($"Unsupported column encoding {kind}");
            }
        }

        private IEnumerable<string> ReadDirectV2()
        {
            var present = ReadBooleanStream(StreamKind.Present);
            var data = ReadBinaryStream(StreamKind.Data);
            var length = ReadNumericStream(StreamKind.Length, false);
            if (data == null || length == null)
                throw new InvalidDataException("DATA and LENGTH streams must be available");

            var stringOffset = 0;
            if (present == null)
            {
                foreach (var len in length)
                {
                    var value = Encoding.UTF8.GetString(data, stringOffset, (int) len);
                    stringOffset += (int) len;
                    yield return value;
                }
            }
            else
            {
                var lengthEnumerator = ((IEnumerable<long>) length).GetEnumerator();
                foreach (var isPresent in present)
                    if (isPresent)
                    {
                        var success = lengthEnumerator.MoveNext();
                        if (!success)
                            throw new InvalidDataException(
                                "The PRESENT data stream's length didn't match the LENGTH stream's length");
                        var len = lengthEnumerator.Current;
                        var value = Encoding.UTF8.GetString(data, stringOffset, (int) len);
                        stringOffset += (int) len;
                        yield return value;
                    }
                    else
                    {
                        yield return null;
                    }
            }
        }

        private IEnumerable<string> ReadDictionaryV2()
        {
            var present = ReadBooleanStream(StreamKind.Present);
            var data = ReadNumericStream(StreamKind.Data, false);
            var dictionaryData = ReadBinaryStream(StreamKind.DictionaryData);
            var length = ReadNumericStream(StreamKind.Length, false);
            if (data == null || dictionaryData == null || length == null)
                throw new InvalidDataException("DATA, DICTIONARY_DATA, and LENGTH streams must be available");

            var dictionary = new List<string>();
            var stringOffset = 0;
            foreach (var len in length)
            {
                var dictionaryValue = Encoding.UTF8.GetString(dictionaryData, stringOffset, (int) len);
                stringOffset += (int) len;
                dictionary.Add(dictionaryValue);
            }

            if (present == null)
            {
                foreach (var value in data)
                    yield return dictionary[(int) value];
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
                        yield return dictionary[(int) valueEnumerator.Current];
                    }
                    else
                    {
                        yield return null;
                    }
            }
        }
    }
}