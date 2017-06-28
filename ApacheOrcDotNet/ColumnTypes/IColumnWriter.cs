using System.Collections.Generic;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.ColumnTypes
{
    public interface IColumnWriter
    {
        List<IStatistics> Statistics { get; }
        ColumnEncodingKind ColumnEncoding { get; }
        IEnumerable<OrcCompressedBuffer> Buffers { get; }
        long CompressedLength { get; }
        uint ColumnId { get; }
        void FlushBuffers();
        void Reset();
    }
}