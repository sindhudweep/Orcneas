﻿using System.Collections.Generic;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class StructWriter : IColumnWriter<object>
    {
        //Assume all root values are present
        public StructWriter(OrcCompressedBufferFactory bufferFactory, uint columnId)
        {
            ColumnId = columnId;
        }

        public List<IStatistics> Statistics { get; } = new List<IStatistics>();
        public long CompressedLength => 0;
        public uint ColumnId { get; }
        public IEnumerable<OrcCompressedBuffer> Buffers => new OrcCompressedBuffer[] { };
        public ColumnEncodingKind ColumnEncoding => ColumnEncodingKind.Direct;

        public void FlushBuffers()
        {
        }

        public void Reset()
        {
            Statistics.Clear();
        }

        public void AddBlock(IList<object> values)
        {
            var stats = new BooleanWriterStatistics();
            Statistics.Add(stats);
            foreach (var buffer in Buffers)
                buffer.AnnotatePosition(stats, 0);

            stats.NumValues += (uint) values.Count;
        }
    }
}