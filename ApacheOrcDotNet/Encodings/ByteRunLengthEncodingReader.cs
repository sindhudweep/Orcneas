using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.Encodings
{
    public class ByteRunLengthEncodingReader
    {
        private readonly Stream _inputStream;

        public ByteRunLengthEncodingReader(Stream inputStream)
        {
            _inputStream = inputStream;
        }

        public IEnumerable<byte> Read()
        {
            while (true)
            {
                var firstByte = _inputStream.ReadByte();
                if (firstByte < 0) //No more data available
                    yield break;

                if (firstByte < 0x80) //A run
                {
                    var numBytes = firstByte + 3;
                    var repeatedByte = _inputStream.CheckedReadByte();
                    for (var i = 0; i < numBytes; i++)
                        yield return repeatedByte;
                }
                else //Literals
                {
                    var numBytes = 0x100 - firstByte;
                    for (var i = 0; i < numBytes; i++)
                        yield return _inputStream.CheckedReadByte();
                }
            }
        }
    }
}