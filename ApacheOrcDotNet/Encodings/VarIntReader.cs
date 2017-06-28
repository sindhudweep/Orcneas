using System.Collections.Generic;
using System.IO;
using System.Numerics;

namespace ApacheOrcDotNet.Encodings
{
    public class VarIntReader
    {
        private readonly Stream _inputStream;

        public VarIntReader(Stream inputStream)
        {
            _inputStream = inputStream;
        }

        public IEnumerable<BigInteger> Read()
        {
            while (true)
            {
                var bigInt = _inputStream.ReadBigVarInt();
                if (bigInt.HasValue)
                    yield return bigInt.Value;
                else
                    yield break;
            }
        }
    }
}