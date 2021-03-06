﻿using System.Collections.Generic;
using System.Linq;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class LongWriter : IColumnWriter<long?>
    {
        private readonly OrcCompressedBuffer _dataBuffer;
        private readonly bool _isNullable;
        private readonly OrcCompressedBuffer _presentBuffer;
        private readonly bool _shouldAlignEncodedValues;

        public LongWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory,
            uint columnId)
        {
            _isNullable = isNullable;
            _shouldAlignEncodedValues = shouldAlignEncodedValues;
            ColumnId = columnId;

            if (_isNullable)
            {
                _presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
                _presentBuffer.MustBeIncluded = false; //If we never have nulls, we won't write this stream
            }
            _dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
        }

        public List<IStatistics> Statistics { get; } = new List<IStatistics>();
        public long CompressedLength => Buffers.Sum(s => s.Length);
        public uint ColumnId { get; }

        public IEnumerable<OrcCompressedBuffer> Buffers => _isNullable
            ? new[] {_presentBuffer, _dataBuffer}
            : new[] {_dataBuffer};

        public ColumnEncodingKind ColumnEncoding => ColumnEncodingKind.DirectV2;

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

        public void AddBlock(IList<long?> values)
        {
            var stats = new LongWriterStatistics();
            Statistics.Add(stats);
            foreach (var buffer in Buffers)
                buffer.AnnotatePosition(stats, 0);

            var valList = new List<long>(values.Count);

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
                    _presentBuffer.MustBeIncluded = true; //A null occurred.  Make sure to write this stream
            }
            else
            {
                foreach (var value in values)
                {
                    stats.AddValue(value);
                    valList.Add(value.Value);
                }
            }

            var valEncoder = new IntegerRunLengthEncodingV2Writer(_dataBuffer);
            valEncoder.Write(valList, true, _shouldAlignEncodedValues);
        }
    }
}