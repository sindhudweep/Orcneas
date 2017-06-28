using System.IO;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet.Stripes
{
    public class StripeStreamReader
    {
        private readonly ulong _compressedLength;
        private readonly CompressionKind _compressionKind;
        private readonly Stream _inputStream;
        private readonly long _inputStreamOffset;

        internal StripeStreamReader(Stream inputStream, uint columnId, StreamKind streamKind,
            ColumnEncodingKind encodingKind, long inputStreamOffset, ulong compressedLength,
            CompressionKind compressionKind)
        {
            _inputStream = inputStream;
            ColumnId = columnId;
            StreamKind = streamKind;
            ColumnEncodingKind = encodingKind;
            _inputStreamOffset = inputStreamOffset;
            _compressedLength = compressedLength;
            _compressionKind = compressionKind;
        }

        public uint ColumnId { get; }
        public StreamKind StreamKind { get; }
        public ColumnEncodingKind ColumnEncodingKind { get; }

        public Stream GetDecompressedStream()
        {
            //TODO move from using Streams to using MemoryMapped files or another data type that decouples the Stream Position from the Read call, allowing re-entrancy
            _inputStream.Seek(_inputStreamOffset, SeekOrigin.Begin);
            var segment = new StreamSegment(_inputStream, (long) _compressedLength, true);
            return OrcCompressedStream.GetDecompressingStream(segment, _compressionKind);
        }
    }
}