﻿using System;
using System.Collections.Generic;
using System.IO;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class BinaryReader : ColumnReader
    {
        public BinaryReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
        {
        }

        public IEnumerable<byte[]> Read()
        {
            var present = ReadBooleanStream(StreamKind.Present);
            var data = ReadBinaryStream(StreamKind.Data);
            var length = ReadNumericStream(StreamKind.Length, false);
            if (data == null || length == null)
                throw new InvalidDataException("DATA and LENGTH streams must be available");

            var byteOffset = 0;
            if (present == null)
            {
                foreach (var len in length)
                {
                    var bytes = new byte[len];
                    Buffer.BlockCopy(data, byteOffset, bytes, 0, (int) len);
                    byteOffset += (int) len;
                    yield return bytes;
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
                        var bytes = new byte[len];
                        Buffer.BlockCopy(data, byteOffset, bytes, 0, (int) len);
                        byteOffset += (int) len;
                        yield return bytes;
                    }
                    else
                    {
                        yield return null;
                    }
            }
        }
    }
}