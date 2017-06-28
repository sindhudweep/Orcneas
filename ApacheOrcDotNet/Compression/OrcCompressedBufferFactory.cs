using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.Compression
{
    public class OrcCompressedBufferFactory
    {
        public OrcCompressedBufferFactory(WriterConfiguration configuration)
        {
            CompressionBlockSize = configuration.BufferSize;
            CompressionKind = configuration.Compress.ToCompressionKind();
            CompressionStrategy = configuration.CompressionStrategy;
        }

        public OrcCompressedBufferFactory(int compressionBlockSize, CompressionKind compressionKind,
            CompressionStrategy compressionStrategy)
        {
            CompressionBlockSize = compressionBlockSize;
            CompressionKind = compressionKind;
            CompressionStrategy = compressionStrategy;
        }

        public int CompressionBlockSize { get; }
        public CompressionKind CompressionKind { get; }
        public CompressionStrategy CompressionStrategy { get; }

        public OrcCompressedBuffer CreateBuffer(StreamKind streamKind)
        {
            return new OrcCompressedBuffer(CompressionBlockSize, CompressionKind, CompressionStrategy, streamKind);
        }

        public OrcCompressedBuffer CreateBuffer()
        {
            return new OrcCompressedBuffer(CompressionBlockSize, CompressionKind, CompressionStrategy);
        }
    }
}