using System;
using Microsoft.Analytics.Interfaces;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using FileTail = ApacheOrcDotNet.FileTail;
using Stream = System.IO.Stream;

namespace Orcneas.Core
{

    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public sealed class OrcExtractor : IExtractor
    {
        public SeekableInputStreamHackMode SeekableStreamHackMode { get; }
        public OrcExtractor(SeekableInputStreamHackMode streamHackMode = SeekableInputStreamHackMode.CopyToMemoryStream)
        {
            SeekableStreamHackMode = streamHackMode;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            if (input.Length == 0)
                yield break;

            using (var stream = GetSeekableStream(input, SeekableStreamHackMode))
                foreach (var row in ExtractInternal(output, stream)) yield return row;
        }

        private static IEnumerable<IRow> ExtractInternal(IUpdatableRow output, Stream input)
        {
            if (!input.CanSeek)
            {
                throw new ArgumentOutOfRangeException(nameof(input), "Input stream must be seekable for ORC reader. Enable the hack to copy to a Memory Stream or to a non-Persisted Memory Mapped file. The hack is the default setting.");
            }
            
            var fileTail = new FileTail(input);
            var stripes = fileTail.GetStripeCollection();

            var columnsToRead = GetIntersectedColumnMetadata(output.Schema, fileTail).ToArray();

            foreach (var stripe in stripes)
            {
                var extractedColumns = ReadStripe(stripe, columnsToRead).ToArray();

                for (int i = 0; i < (int) stripe.NumRows; i++)
                {
                    foreach (var col in extractedColumns)
                    {
                        var outputColumn = col.Item1.USqlProjectionColumnIndex;
                        var value = col.Item2?.GetValue(i) ?? col.Item1.USqlProjectionColumn.DefaultValue;
                        output.Set(outputColumn, value);
                    }
                    yield return output.AsReadOnly();
                }
            }
        }

        private static Stream GetSeekableStream(IUnstructuredReader input, SeekableInputStreamHackMode inputStreamHackMode)
        {
            switch (inputStreamHackMode)
            {
                case SeekableInputStreamHackMode.Disabled:
                    return input.BaseStream;
                case SeekableInputStreamHackMode.CopyToMemoryStream:
                    var ms = new MemoryStream((int)input.Length);
                    input.BaseStream.CopyTo(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    return ms;
                case SeekableInputStreamHackMode.ReflectIntoBaseStream:
                    throw new NotImplementedException();
                default:
                    throw new NotImplementedException();
            }
        }

        private static List<(ProjectedColumnMetadata, Array)> ReadStripe(
            StripeReader stripe, 
            IEnumerable<ProjectedColumnMetadata> projectedColumns
        )
        {
            var stripeStreams = stripe.GetStripeStreamCollection();
            var results = new List<(ProjectedColumnMetadata, Array)>();
            foreach (var column in projectedColumns)
            {
                switch (column.ColumnTypeKind)
                {
                    case ColumnTypeKind.Double:
                        var doubleReader = new ApacheOrcDotNet.ColumnTypes.DoubleReader(stripeStreams, column.OrcColumnIndex);
                        var doubleData = doubleReader.Read().ToArray();
                        results.Add((column, doubleData));
                        break;
                    case ColumnTypeKind.String:
                        var stringReader = new ApacheOrcDotNet.ColumnTypes.StringReader(stripeStreams, column.OrcColumnIndex);
                        var stringData = stringReader.Read().ToArray();
                        results.Add((column, stringData));
                        break;
                    case ColumnTypeKind.Date:
                        var dateReader = new ApacheOrcDotNet.ColumnTypes.DateReader(stripeStreams, column.OrcColumnIndex);
                        var dateData = dateReader.Read().ToArray();
                        results.Add((column, dateData));
                        break;
                    case ColumnTypeKind.Timestamp:
                        var timeReader = new ApacheOrcDotNet.ColumnTypes.TimestampReader(stripeStreams, column.OrcColumnIndex);
                        var timeData = timeReader.Read().ToArray();
                        results.Add((column, timeData));
                        break;
                    default:
                        throw new NotImplementedException($"Unsupported Column Type. {column.ColumnTypeKind}");
                }
            }
            return results;
        }

        private static IEnumerable<ProjectedColumnMetadata> GetIntersectedColumnMetadata(ISchema usqlSchema, FileTail orcFileTail)
        {
            var orcColumnNames = orcFileTail.Footer.Types[0].FieldNames;

            var orcColumnMetadata =
                orcFileTail.Footer.Types.Skip(1).Select((ct, i) => new
                {
                    ColumnTypeKind = ct.Kind,
                    OrcColumnName = orcColumnNames[i],
                    OrcColumnIndex = (uint) (i + 1)
                }).ToDictionary(x => x.OrcColumnName, x => x);

            for (int i = 0; i < usqlSchema.Count; i++)
            {
                var uSqlColumn = usqlSchema[i];
                if (orcColumnMetadata.ContainsKey(uSqlColumn.Name))
                {
                    var orcMeta = orcColumnMetadata[uSqlColumn.Name];
                    yield return new ProjectedColumnMetadata
                    {
                        ColumnTypeKind = orcMeta.ColumnTypeKind,
                        OrcColumnIndex = orcMeta.OrcColumnIndex,
                        OrcColumnName = orcMeta.OrcColumnName,
                        USqlProjectionColumn = uSqlColumn,
                        USqlProjectionColumnIndex = i
                    };
                }
            }
        }

        public enum SeekableInputStreamHackMode
        {
            Disabled = 0, //Will not work with current ADLA code.
            CopyToMemoryStream = 1, //Default, but incures the cost of allocating and copying into a memory stream for seeking purposes.
            ReflectIntoBaseStream = 2 //Doesn't copy the entire input stream into memory, but brittle.
        }
    }
}