using System;
using System.Collections.Generic;
using System.IO;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet
{
    public class OrcWriter<T> : IOrcWriter<T>
    {
        private readonly OrcCompressedBufferFactory _bufferFactory;
        private readonly string _magic = "ORC";
        private readonly Stream _outputStream;
        private readonly StripeWriter _stripeWriter;

        private readonly List<uint> _version = new List<uint> {0, 12};
        private readonly uint _writerVersion = 5;

        public OrcWriter(Stream outputStream, WriterConfiguration configuration)
        {
            _outputStream = outputStream;

            _bufferFactory = new OrcCompressedBufferFactory(configuration);
            _stripeWriter = new StripeWriter(
                typeof(T),
                outputStream,
                configuration.EncodingStrategy == EncodingStrategy.Speed,
                configuration.DictionaryKeySizeThreshold,
                configuration.DefaultDecimalPrecision,
                configuration.DefaultDecimalScale,
                _bufferFactory,
                configuration.RowIndexStride,
                configuration.StripeSize
            );

            WriteHeader();
        }

        public void AddRow(T row)
        {
            _stripeWriter.AddRows(new object[] {row});
        }

        public void AddRows(IEnumerable<T> rows)
        {
            _stripeWriter.AddRows((IEnumerable<object>) rows);
        }

        public void AddUserMetadata(string key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _stripeWriter.RowAddingCompleted();

            WriteTail();
        }

        private void WriteTail()
        {
            var metadata = _stripeWriter.GetMetadata();
            var footer = _stripeWriter.GetFooter();
            footer.HeaderLength = (ulong) _magic.Length;

            long metadataLength, footerLength;
            _bufferFactory.SerializeAndCompressTo(_outputStream, metadata, out metadataLength);
            _bufferFactory.SerializeAndCompressTo(_outputStream, footer, out footerLength);

            var postScript = GetPostscript((ulong) footerLength, (ulong) metadataLength);
            var postScriptStream = new MemoryStream();
            StaticProtoBuf.Serializer.Serialize(postScriptStream, postScript);
            postScriptStream.Seek(0, SeekOrigin.Begin);
            postScriptStream.CopyTo(_outputStream);

            if (postScriptStream.Length > 255)
                throw new InvalidDataException("Invalid Postscript length");

            _outputStream.WriteByte((byte) postScriptStream.Length);
        }

        private PostScript GetPostscript(ulong footerLength, ulong metadataLength)
        {
            return new PostScript
            {
                FooterLength = footerLength,
                Compression = _bufferFactory.CompressionKind,
                CompressionBlockSize = (ulong) _bufferFactory.CompressionBlockSize,
                Version = _version,
                MetadataLength = metadataLength,
                WriterVersion = _writerVersion,
                Magic = _magic
            };
        }

        private void WriteHeader()
        {
            var magic = new[] {(byte) 'O', (byte) 'R', (byte) 'C'};
            _outputStream.Write(magic, 0, magic.Length);
        }
    }

}