using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using ProtoBuf;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet
{
    public class FileTail : IDisposable
    {
        private readonly MemoryMappedFile _input;
        private readonly MemoryMappedViewStream _inputStream;
        public PostScript PostScript { get; }
        public Footer Footer { get; }
        public Metadata Metadata { get; }


        private static MemoryMappedFile AsMemoryMappedFile(Func<Stream> streamResolver, Func<long> lengthProvider)
        {
            var stream = streamResolver();
            var length = lengthProvider();
            var mmf = MemoryMappedFile.CreateNew(Guid.NewGuid().ToString(), length);
            using (var mmvs = mmf.CreateViewStream(0, length))
            {
                stream.CopyTo(mmvs);
            }
            return mmf;
        }

        public FileTail(Stream input) : this(() => input, () => input.Length) { }
        public FileTail(Func<Stream> streamResolver, Func<long> lengthProvider) : this(AsMemoryMappedFile(streamResolver, lengthProvider), lengthProvider()) { }
        private FileTail(MemoryMappedFile input, long length)
        {
            _input = input;
            _inputStream = _input.CreateViewStream(0, length);
            var (postScript, postScriptLength) =_inputStream.ReadPostScript();
            PostScript = postScript;
            Footer = ReadFooter(PostScript, postScriptLength);
            Metadata = ReadMetadata(PostScript, postScriptLength);
        }

        private Footer ReadFooter(PostScript postScript, byte postScriptLength)
        {
            var offset = -1 -postScriptLength - (long)postScript.FooterLength;
            _inputStream.Seek(offset, SeekOrigin.End);
            var compressedStream = new StreamSegment(_inputStream, (long) postScript.FooterLength, true);
            var footerStream = OrcCompressedStream.GetDecompressingStream(compressedStream, postScript.Compression);

            return Serializer.Deserialize<Footer>(footerStream);
        }

        private Metadata ReadMetadata(PostScript postScript, byte postScriptLength)
        {
            var offset = -1 -postScriptLength -(long)postScript.FooterLength - (long)postScript.MetadataLength;
            _inputStream.Seek(offset, SeekOrigin.End);
            var compressedStream = new StreamSegment(_inputStream, (long)postScript.MetadataLength, true);
            var metadataStream = OrcCompressedStream.GetDecompressingStream(compressedStream, postScript.Compression);

            return Serializer.Deserialize<Metadata>(metadataStream);
        }

        public StripeReaderCollection GetStripeCollection()
        {
            return new StripeReaderCollection(_inputStream, Footer, PostScript.Compression);
        }

        public void Dispose()
        {
            _inputStream.Dispose();
            _input.Dispose();
        }
    }
}