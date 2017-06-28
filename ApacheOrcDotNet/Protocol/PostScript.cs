﻿using System.Collections.Generic;
using System.IO;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Infrastructure;
using ProtoBuf;

namespace ApacheOrcDotNet.Protocol
{
    public enum CompressionKind
    {
        [ProtoEnum(Name = @"NONE", Value = 0)] None = 0,
        [ProtoEnum(Name = @"ZLIB", Value = 1)] Zlib = 1,
        [ProtoEnum(Name = @"SNAPPY", Value = 2)] Snappy = 2,
        [ProtoEnum(Name = @"LZO", Value = 3)] Lzo = 3,
        [ProtoEnum(Name = @"LZ4", Value = 4)] Lz4 = 4,
        [ProtoEnum(Name = @"ZSTD", Value = 5)] Zstd = 5
    }

    [ProtoContract]
    public class PostScript
    {
        [ProtoMember(1)]
        public ulong FooterLength { get; set; }

        [ProtoMember(2)]
        public CompressionKind Compression { get; set; }

        [ProtoMember(3)]
        public ulong CompressionBlockSize { get; set; }

        [ProtoMember(4, IsPacked = true)]
        public List<uint> Version { get; set; } = new List<uint>();

        public uint? VersionMajor => Version.Count > 0 ? (uint?) Version[0] : null;
        public uint? VersionMinor => Version.Count > 1 ? (uint?) Version[1] : null;

        [ProtoMember(5)]
        public ulong MetadataLength { get; set; }

        [ProtoMember(6)]
        public uint WriterVersion { get; set; }


        [ProtoMember(8000)]
        public string Magic { get; set; }
    }

    public static class PostScriptHelpers
    {
        public static (PostScript, byte) ReadPostScript(this System.IO.Stream inputStream)
        {
            inputStream.Seek(-1, SeekOrigin.End);
            byte postScriptLength = inputStream.CheckedReadByte();

            inputStream.Seek(-1 - postScriptLength, SeekOrigin.End);
            var stream = new StreamSegment(inputStream, postScriptLength, true);

            var postScript = Serializer.Deserialize<PostScript>(stream);

            if (postScript.Magic != "ORC")
                throw new InvalidDataException("Postscript didn't contain magic bytes");

            return (postScript, postScriptLength);
        }
    }

}