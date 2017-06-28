﻿using System.Collections.Generic;
using System.Linq;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class FloatWriter : IColumnWriter<float?>
    {
        private readonly OrcCompressedBuffer _dataBuffer;
        private readonly bool _isNullable;
        private readonly OrcCompressedBuffer _presentBuffer;

        public FloatWriter(bool isNullable, OrcCompressedBufferFactory bufferFactory, uint columnId)
        {
            _isNullable = isNullable;
            ColumnId = columnId;

            if (_isNullable)
            {
                _presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
                _presentBuffer.MustBeIncluded = false;
            }
            _dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
        }

        public List<IStatistics> Statistics { get; } = new List<IStatistics>();
        public long CompressedLength => Buffers.Sum(s => s.Length);
        public uint ColumnId { get; }

        public IEnumerable<OrcCompressedBuffer> Buffers => _isNullable
            ? new[] {_presentBuffer, _dataBuffer}
            : new[] {_dataBuffer};

        public ColumnEncodingKind ColumnEncoding => ColumnEncodingKind.Direct;

        public void FlushBuffers()
        {
            foreach (var buffer in Buffers)
                buffer.Flush();
        }

        public void Reset()
        {
            foreach (var buffer in Buffers)
                buffer.Reset();
            if (_isNullable)
                _presentBuffer.MustBeIncluded = false;
            Statistics.Clear();
        }

        public void AddBlock(IList<float?> values)
        {
            var stats = new DoubleWriterStatistics();
            Statistics.Add(stats);
            foreach (var buffer in Buffers)
                buffer.AnnotatePosition(stats, 0);

            var valList = new List<float>(values.Count);

            if (_isNullable)
            {
                var presentList = new List<bool>(values.Count);

                foreach (var value in values)
                {
                    stats.AddValue(value);
                    if (value.HasValue)
                        valList.Add(value.Value);
                    presentList.Add(value.HasValue);
                }

                var presentEncoder = new BitWriter(_presentBuffer);
                presentEncoder.Write(presentList);
                if (stats.HasNull)
                    _presentBuffer.MustBeIncluded = true;
            }
            else
            {
                foreach (var value in values)
                {
                    stats.AddValue(value);
                    valList.Add(value.Value);
                }
            }

            foreach (var value in valList)
                _dataBuffer.WriteFloatBE(value);
        }
    }
}