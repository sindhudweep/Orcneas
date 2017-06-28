﻿using System;
using System.IO;
using ApacheOrcDotNet.Protocol;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet.Compression
{
    public class OrcCompressedBuffer : Stream
    {
        private readonly MemoryStream _compressedBuffer = new MemoryStream();
        private readonly int _compressionBlockSize;
        private readonly CompressionKind _compressionKind;
        private readonly CompressionStrategy _compressionStrategy;
        private readonly MemoryStream _currentBlock = new MemoryStream();

        public OrcCompressedBuffer(int compressionBlockSize, CompressionKind compressionKind,
            CompressionStrategy compressionStrategy)
        {
            _compressionBlockSize = compressionBlockSize;
            _compressionKind = compressionKind;
            _compressionStrategy = compressionStrategy;
        }

        public OrcCompressedBuffer(int compressionBlockSize, CompressionKind compressionKind,
            CompressionStrategy compressionStrategy, StreamKind streamKind)
            : this(compressionBlockSize, compressionKind, compressionStrategy)
        {
            StreamKind = streamKind;
        }

        public StreamKind StreamKind { get; }
        public bool MustBeIncluded { get; set; } = true;

        public override long Length => _compressedBuffer.Length;

        public override void Write(byte[] buffer, int offset, int count)
        {
            var spaceRemaining = _compressionBlockSize - (int) _currentBlock.Length;
            if (spaceRemaining < 0)
                throw new ArithmeticException("A code error has led to negative space remaining");

            if (count >= spaceRemaining)
            {
                _currentBlock.Write(buffer, offset, spaceRemaining);
                Flush();
                count -= spaceRemaining;
                offset += spaceRemaining;
            }
            if (count > 0)
                _currentBlock.Write(buffer, offset, count);
        }

        public override void Flush()
        {
            if (_currentBlock.Length == 0)
                return;

            //Compress the encoded block and write it to the CompressedBuffer
            OrcCompressedStream.CompressCopyTo(_currentBlock, _compressedBuffer, _compressionKind,
                _compressionStrategy);
            _currentBlock.SetLength(0);
        }

        public new void CopyTo(Stream destination)
        {
            Flush();
            _compressedBuffer.Seek(0, SeekOrigin.Begin);
            _compressedBuffer.CopyTo(destination);
        }

        public void AnnotatePosition(IStatistics statistics, long rleValuesToConsume)
        {
            if (_compressionKind == CompressionKind.None)
                statistics.AnnotatePosition(Length + _currentBlock.Length,
                    rleValuesToConsume); //If we're not compressing, output the total length as a single value
            else
                statistics.AnnotatePosition(Length, _currentBlock.Length, rleValuesToConsume);
        }

        public void Reset()
        {
            _compressedBuffer.SetLength(0);
            _currentBlock.SetLength(0);
        }


        #region Stream Implementation

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;

        public override long Position
        {
            get => throw new NotImplementedException();

            set => throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}