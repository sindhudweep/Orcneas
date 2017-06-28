using System.Collections.Generic;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class ColumnWriterStatistics
    {
        public List<long> CompressedBufferOffsets { get; } = new List<long>();
        public List<long> DecompressedOffsets { get; } = new List<long>();
        public List<long> RleValuesToConsume { get; } = new List<long>();

        public void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume)
        {
            CompressedBufferOffsets.Add(compressedBufferOffset);
            DecompressedOffsets.Add(decompressedOffset);
            RleValuesToConsume.Add(rleValuesToConsume);
        }

        public void AnnotatePosition(long uncompressedOffset, long rleValuesToConsume)
        {
            CompressedBufferOffsets.Add(uncompressedOffset);
            RleValuesToConsume.Add(rleValuesToConsume);
        }

        public void FillPositionList(List<ulong> positions)
        {
            //If we weren't dealing with compressed data, only two values are written rather than three
            var haveSecondValues = DecompressedOffsets.Count != 0;
            for (var i = 0; i < CompressedBufferOffsets.Count; i++)
            {
                positions.Add((ulong) CompressedBufferOffsets[i]);
                if (haveSecondValues)
                    positions.Add((ulong) DecompressedOffsets[i]);
                positions.Add((ulong) RleValuesToConsume[i]);
            }
        }
    }
}