using System.IO;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ProtoBuf;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet.Stripes
{
    public class StripeReader
    {
        private readonly CompressionKind _compressionKind;
        private readonly ulong _dataLength;
        private readonly ulong _dataOffset;
        private readonly ulong _footerLength;
        private readonly ulong _footerOffset;
        private readonly ulong _indexLength;
        private readonly ulong _indexOffset;
        private readonly Stream _inputStream;

        internal StripeReader(Stream inputStream, ulong indexOffset, ulong indexLength, ulong dataOffset,
            ulong dataLength, ulong footerOffset, ulong footerLength, ulong numRows, CompressionKind compressionKind)
        {
            _inputStream = inputStream;
            _indexOffset = indexOffset;
            _indexLength = indexLength;
            _dataOffset = dataOffset;
            _dataLength = dataLength;
            _footerOffset = footerOffset;
            _footerLength = footerLength;
            NumRows = numRows;
            _compressionKind = compressionKind;
        }

        public ulong NumRows { get; }

        private Stream GetStream(ulong offset, ulong length)
        {
            //TODO move from using Streams to using MemoryMapped files or another data type that decouples the Stream Position from the Read call, allowing re-entrancy
            _inputStream.Seek((long) offset, SeekOrigin.Begin);
            var segment = new StreamSegment(_inputStream, (long) length, true);
            return OrcCompressedStream.GetDecompressingStream(segment, _compressionKind);
        }

        private StripeFooter GetStripeFooter()
        {
            var stream = GetStream(_footerOffset, _footerLength);
            return Serializer.Deserialize<StripeFooter>(stream);
        }

        public Stream GetIndexStream()
        {
            return GetStream(_indexOffset, _indexLength);
        }

        public StripeStreamReaderCollection GetStripeStreamCollection()
        {
            var footer = GetStripeFooter();
            return new StripeStreamReaderCollection(_inputStream, footer, (long) _indexOffset, _compressionKind);
        }
    }
}