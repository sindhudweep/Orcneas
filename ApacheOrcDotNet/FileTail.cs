using System.IO;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using ProtoBuf;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet
{
    public class FileTail
    {
        private readonly Stream _inputStream;
        public PostScript PostScript { get; }
        public Footer Footer { get; }
        public Metadata Metadata { get; }

        public FileTail(Stream inputStream)
        {
            _inputStream = inputStream;
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
    }
}