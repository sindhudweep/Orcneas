using System.Collections.Generic;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet
{
    public interface IStatistics
    {
        void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume);
        void AnnotatePosition(long uncompressedOffset, long rleValuesToConsume);
        void FillColumnStatistics(ColumnStatistics columnStatistics);
        void FillPositionList(List<ulong> positions);
    }
}