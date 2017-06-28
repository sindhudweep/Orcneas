using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using Stream = System.IO.Stream;

namespace ApacheOrcDotNet.Stripes
{
    public class StripeWriter
    {
        private readonly OrcCompressedBufferFactory _bufferFactory;
        private readonly List<ColumnWriterDetails> _columnWriters = new List<ColumnWriterDetails>();
        private readonly int _defaultDecimalPrecision;
        private readonly int _defaultDecimalScale;
        private readonly Stream _outputStream;
        private readonly bool _shouldAlignNumericValues;
        private readonly int _strideLength;
        private readonly long _stripeLength;
        private readonly List<StripeStatistics> _stripeStats = new List<StripeStatistics>();
        private readonly string _typeName;
        private readonly double _uniqueStringThresholdRatio;
        private long _contentLength;

        private bool _rowAddingCompleted;
        private long _rowsInFile;
        private int _rowsInStride;
        private long _rowsInStripe;
        private readonly List<StripeInformation> _stripeInformations = new List<StripeInformation>();

        public StripeWriter(Type pocoType, Stream outputStream, bool shouldAlignNumericValues,
            double uniqueStringThresholdRatio, int defaultDecimalPrecision, int defaultDecimalScale,
            OrcCompressedBufferFactory bufferFactory, int strideLength, long stripeLength)
        {
            _typeName = pocoType.Name;
            _outputStream = outputStream;
            _shouldAlignNumericValues = shouldAlignNumericValues;
            _uniqueStringThresholdRatio = uniqueStringThresholdRatio;
            _defaultDecimalPrecision = defaultDecimalPrecision;
            _defaultDecimalScale = defaultDecimalScale;
            _bufferFactory = bufferFactory;
            _strideLength = strideLength;
            _stripeLength = stripeLength;

            CreateColumnWriters(pocoType);
        }

        public void AddRows(IEnumerable<object> rows)
        {
            if (_rowAddingCompleted)
                throw new InvalidOperationException("Row adding as been completed");

            foreach (var row in rows)
            {
                foreach (var columnWriter in _columnWriters)
                    columnWriter.AddValueToState(row);

                if (++_rowsInStride >= _strideLength)
                    CompleteStride();
            }
        }

        public void RowAddingCompleted()
        {
            if (_rowsInStride != 0)
                CompleteStride();
            if (_rowsInStripe != 0)
                CompleteStripe();

            _contentLength = _outputStream.Position;
            _rowAddingCompleted = true;
        }

        public Footer GetFooter()
        {
            if (!_rowAddingCompleted)
                throw new InvalidOperationException("Row adding not completed");

            return new Footer
            {
                ContentLength = (ulong) _contentLength,
                NumberOfRows = (ulong) _rowsInFile,
                RowIndexStride = (uint) _strideLength,
                Stripes = _stripeInformations,
                Statistics = _columnWriters.Select(c => c.FileStatistics).ToList(),
                Types = GetColumnTypes().ToList()
            };
        }

        public Metadata GetMetadata()
        {
            if (!_rowAddingCompleted)
                throw new InvalidOperationException("Row adding not completed");

            return new Metadata
            {
                StripeStats = _stripeStats
            };
        }

        private void CompleteStride()
        {
            foreach (var columnWriter in _columnWriters)
                columnWriter.WriteValuesFromState();

            var totalStripeLength = _columnWriters.Sum(writer => writer.ColumnWriter.CompressedLength);
            if (totalStripeLength > _stripeLength)
                CompleteStripe();

            _rowsInStripe += _rowsInStride;
            _rowsInStride = 0;
        }

        private void CompleteStripe()
        {
            var stripeFooter = new StripeFooter();
            var stripeStats = new StripeStatistics();

            //Columns
            foreach (var writer in _columnWriters)
            {
                writer.ColumnWriter.FlushBuffers();
                var dictionaryLength =
                    (writer.ColumnWriter as StringWriter)?.DictionaryLength ??
                    0; //DictionaryLength is only used by StringWriter
                stripeFooter.AddColumn(writer.ColumnWriter.ColumnEncoding, dictionaryLength);
            }

            var stripeInformation = new StripeInformation
            {
                Offset = (ulong) _outputStream.Position,
                NumberOfRows = (ulong) _rowsInStripe
            };

            //Indexes
            foreach (var writer in _columnWriters)
            {
                //Write the index buffer
                var indexBuffer = _bufferFactory.CreateBuffer(StreamKind.RowIndex);
                writer.ColumnWriter.Statistics.WriteToBuffer(indexBuffer);
                indexBuffer.CopyTo(_outputStream);

                //Add the index to the footer
                stripeFooter.AddDataStream(writer.ColumnWriter.ColumnId, indexBuffer);

                //Collect summary statistics
                var columnStats = new ColumnStatistics();
                foreach (var stats in writer.ColumnWriter.Statistics)
                {
                    stats.FillColumnStatistics(columnStats);
                    stats.FillColumnStatistics(writer.FileStatistics);
                }
                stripeStats.ColStats.Add(columnStats);
            }
            _stripeStats.Add(stripeStats);

            stripeInformation.IndexLength = (ulong) _outputStream.Position - stripeInformation.Offset;

            //Data streams
            foreach (var writer in _columnWriters)
            foreach (var buffer in writer.ColumnWriter.Buffers)
            {
                if (!buffer.MustBeIncluded)
                    continue;
                buffer.CopyTo(_outputStream);
                stripeFooter.AddDataStream(writer.ColumnWriter.ColumnId, buffer);
            }

            stripeInformation.DataLength = (ulong) _outputStream.Position - stripeInformation.IndexLength -
                                           stripeInformation.Offset;

            //Footer
            long footerLength;
            _bufferFactory.SerializeAndCompressTo(_outputStream, stripeFooter, out footerLength);
            stripeInformation.FooterLength = (ulong) footerLength;

            _stripeInformations.Add(stripeInformation);

            _rowsInFile += _rowsInStripe;
            _rowsInStripe = 0;
            foreach (var writer in _columnWriters)
                writer.ColumnWriter.Reset();
        }

        private void CreateColumnWriters(Type type)
        {
            uint columnId = 1;
            foreach (var propertyInfo in GetPublicPropertiesFromPoco(type.GetTypeInfo()))
            {
                var columnWriterAndAction = GetColumnWriterDetails(propertyInfo, columnId++);
                _columnWriters.Add(columnWriterAndAction);
            }
            _columnWriters.Insert(0, GetStructColumnWriter()); //Add the struct column at the beginning
        }

        private IEnumerable<ColumnType> GetColumnTypes()
        {
            foreach (var column in _columnWriters)
                yield return column.ColumnType;
        }

        private static IEnumerable<PropertyInfo> GetPublicPropertiesFromPoco(TypeInfo pocoTypeInfo)
        {
            if (pocoTypeInfo.BaseType != null)
                foreach (var property in GetPublicPropertiesFromPoco(pocoTypeInfo.BaseType.GetTypeInfo()))
                    yield return property;

            foreach (var property in pocoTypeInfo.DeclaredProperties)
                if (property.GetMethod != null)
                    yield return property;
        }

        private static bool IsNullable(TypeInfo propertyTypeInfo)
        {
            return propertyTypeInfo.IsGenericType
                   && propertyTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        private static T GetValue<T>(object classInstance, PropertyInfo property)
        {
            return (T) property.GetValue(classInstance); //TODO make this emit IL to avoid the boxing of value-type T
        }

        private ColumnWriterDetails GetColumnWriterDetails(PropertyInfo propertyInfo, uint columnId)
        {
            var propertyType = propertyInfo.PropertyType;

            //TODO move this to a pattern match switch
            if (propertyType == typeof(int))
                return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<int>(classInstance, propertyInfo), ColumnTypeKind.Int);
            if (propertyType == typeof(long))
                return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<long>(classInstance, propertyInfo), ColumnTypeKind.Long);
            if (propertyType == typeof(short))
                return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<short>(classInstance, propertyInfo), ColumnTypeKind.Short);
            if (propertyType == typeof(uint))
                return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<uint>(classInstance, propertyInfo), ColumnTypeKind.Int);
            if (propertyType == typeof(ulong))
                return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo,
                    classInstance => (long) GetValue<ulong>(classInstance, propertyInfo), ColumnTypeKind.Long);
            if (propertyType == typeof(ushort))
                return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<ushort>(classInstance, propertyInfo), ColumnTypeKind.Short);
            if (propertyType == typeof(int?))
                return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<int?>(classInstance, propertyInfo), ColumnTypeKind.Int);
            if (propertyType == typeof(long?))
                return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<long?>(classInstance, propertyInfo), ColumnTypeKind.Long);
            if (propertyType == typeof(short?))
                return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<short?>(classInstance, propertyInfo), ColumnTypeKind.Short);
            if (propertyType == typeof(uint?))
                return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<uint?>(classInstance, propertyInfo), ColumnTypeKind.Int);
            if (propertyType == typeof(ulong?))
                return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo,
                    classInstance => (long?) GetValue<ulong?>(classInstance, propertyInfo), ColumnTypeKind.Long);
            if (propertyType == typeof(ushort?))
                return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<ushort?>(classInstance, propertyInfo), ColumnTypeKind.Short);
            if (propertyType == typeof(byte))
                return GetColumnWriterDetails(GetByteColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<byte>(classInstance, propertyInfo), ColumnTypeKind.Byte);
            if (propertyType == typeof(sbyte))
                return GetColumnWriterDetails(GetByteColumnWriter(false, columnId), propertyInfo,
                    classInstance => (byte) GetValue<sbyte>(classInstance, propertyInfo), ColumnTypeKind.Byte);
            if (propertyType == typeof(byte?))
                return GetColumnWriterDetails(GetByteColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<byte?>(classInstance, propertyInfo), ColumnTypeKind.Byte);
            if (propertyType == typeof(sbyte?))
                return GetColumnWriterDetails(GetByteColumnWriter(true, columnId), propertyInfo,
                    classInstance => (byte?) GetValue<sbyte?>(classInstance, propertyInfo), ColumnTypeKind.Byte);
            if (propertyType == typeof(bool))
                return GetColumnWriterDetails(GetBooleanColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<bool>(classInstance, propertyInfo), ColumnTypeKind.Boolean);
            if (propertyType == typeof(bool?))
                return GetColumnWriterDetails(GetBooleanColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<bool?>(classInstance, propertyInfo), ColumnTypeKind.Boolean);
            if (propertyType == typeof(float))
                return GetColumnWriterDetails(GetFloatColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<float>(classInstance, propertyInfo), ColumnTypeKind.Float);
            if (propertyType == typeof(float?))
                return GetColumnWriterDetails(GetFloatColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<float?>(classInstance, propertyInfo), ColumnTypeKind.Float);
            if (propertyType == typeof(double))
                return GetColumnWriterDetails(GetDoubleColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<double>(classInstance, propertyInfo), ColumnTypeKind.Double);
            if (propertyType == typeof(double?))
                return GetColumnWriterDetails(GetDoubleColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<double?>(classInstance, propertyInfo), ColumnTypeKind.Double);
            if (propertyType == typeof(byte[]))
                return GetColumnWriterDetails(GetBinaryColumnWriter(columnId), propertyInfo,
                    classInstance => GetValue<byte[]>(classInstance, propertyInfo), ColumnTypeKind.Binary);
            if (propertyType == typeof(decimal))
                return GetDecimalColumnWriterDetails(false, columnId, propertyInfo,
                    classInstance => GetValue<decimal>(classInstance, propertyInfo));
            if (propertyType == typeof(decimal?))
                return GetDecimalColumnWriterDetails(true, columnId, propertyInfo,
                    classInstance => GetValue<decimal?>(classInstance, propertyInfo));
            if (propertyType == typeof(DateTime))
                return GetColumnWriterDetails(GetTimestampColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<DateTime>(classInstance, propertyInfo), ColumnTypeKind.Timestamp);
            if (propertyType == typeof(DateTime?))
                return GetColumnWriterDetails(GetTimestampColumnWriter(true, columnId), propertyInfo,
                    classInstance => GetValue<DateTime?>(classInstance, propertyInfo), ColumnTypeKind.Timestamp);
            if (propertyType == typeof(DateTimeOffset))
                return GetColumnWriterDetails(GetTimestampColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<DateTimeOffset>(classInstance, propertyInfo).UtcDateTime,
                    ColumnTypeKind.Timestamp);
            if (propertyType == typeof(DateTimeOffset?))
                return GetColumnWriterDetails(GetTimestampColumnWriter(false, columnId), propertyInfo,
                    classInstance => GetValue<DateTimeOffset?>(classInstance, propertyInfo)?.UtcDateTime,
                    ColumnTypeKind.Timestamp);
            if (propertyType == typeof(string))
                return GetColumnWriterDetails(GetStringColumnWriter(columnId), propertyInfo,
                    classInstance => GetValue<string>(classInstance, propertyInfo), ColumnTypeKind.String);

            throw new NotImplementedException($"Only basic types are supported. Unable to handle type {propertyType}");
        }

        private ColumnWriterDetails GetStructColumnWriter()
        {
            var columnWriter = new StructWriter(_bufferFactory, 0);
            var state = new List<object>();

            var structColumnType = new ColumnType
            {
                Kind = ColumnTypeKind.Struct
            };
            foreach (var column in _columnWriters)
            {
                structColumnType.FieldNames.Add(column.PropertyName);
                structColumnType.SubTypes.Add(column.ColumnWriter.ColumnId);
            }

            return new ColumnWriterDetails
            {
                PropertyName = _typeName,
                ColumnWriter = columnWriter,
                AddValueToState = classInstance => { state.Add(classInstance); },
                WriteValuesFromState = () =>
                {
                    columnWriter.AddBlock(state);
                    state.Clear();
                },
                ColumnType = structColumnType
            };
        }

        private ColumnWriterDetails GetColumnWriterDetails<T>(IColumnWriter<T> columnWriter, PropertyInfo propertyInfo,
            Func<object, T> valueGetter, ColumnTypeKind columnKind)
        {
            var state = new List<T>();
            return new ColumnWriterDetails
            {
                PropertyName = propertyInfo.Name,
                ColumnWriter = columnWriter,
                AddValueToState = classInstance =>
                {
                    var value = valueGetter(classInstance);
                    state.Add(value);
                },
                WriteValuesFromState = () =>
                {
                    columnWriter.AddBlock(state);
                    state.Clear();
                },
                ColumnType = new ColumnType
                {
                    Kind = columnKind
                }
            };
        }

        private ColumnWriterDetails GetDecimalColumnWriterDetails(bool isNullable, uint columnId,
            PropertyInfo propertyInfo, Func<object, decimal?> valueGetter)
        {
            //TODO add two options to configure Precision and Scale, via an attribute on the property, and via a fluent configuration source
            var precision = _defaultDecimalPrecision;
            var scale = _defaultDecimalScale;

            var state = new List<decimal?>();
            var columnWriter = new DecimalWriter(isNullable, _shouldAlignNumericValues, precision, scale,
                _bufferFactory, columnId);
            return new ColumnWriterDetails
            {
                PropertyName = propertyInfo.Name,
                ColumnWriter = columnWriter,
                AddValueToState = classInstance =>
                {
                    var value = valueGetter(classInstance);
                    state.Add(value);
                },
                WriteValuesFromState = () =>
                {
                    columnWriter.AddBlock(state);
                    state.Clear();
                },
                ColumnType = new ColumnType
                {
                    Kind = ColumnTypeKind.Decimal,
                    Precision = (uint) precision,
                    Scale = (uint) scale
                }
            };
        }

        private IColumnWriter<long?> GetLongColumnWriter(bool isNullable, uint columnId)
        {
            return new LongWriter(isNullable, _shouldAlignNumericValues, _bufferFactory, columnId);
        }

        private IColumnWriter<byte?> GetByteColumnWriter(bool isNullable, uint columnId)
        {
            return new ByteWriter(isNullable, _bufferFactory, columnId);
        }

        private IColumnWriter<bool?> GetBooleanColumnWriter(bool isNullable, uint columnId)
        {
            return new BooleanWriter(isNullable, _bufferFactory, columnId);
        }

        private IColumnWriter<float?> GetFloatColumnWriter(bool isNullable, uint columnId)
        {
            return new FloatWriter(isNullable, _bufferFactory, columnId);
        }

        private IColumnWriter<double?> GetDoubleColumnWriter(bool isNullable, uint columnId)
        {
            return new DoubleWriter(isNullable, _bufferFactory, columnId);
        }

        private IColumnWriter<byte[]> GetBinaryColumnWriter(uint columnId)
        {
            return new BinaryWriter(_shouldAlignNumericValues, _bufferFactory, columnId);
        }

        private IColumnWriter<DateTime?> GetTimestampColumnWriter(bool isNullable, uint columnId)
        {
            return new TimestampWriter(isNullable, _shouldAlignNumericValues, _bufferFactory, columnId);
        }

        private IColumnWriter<string> GetStringColumnWriter(uint columnId)
        {
            //TODO consider if we need separate configuration options for aligning lengths vs lookup values
            return new StringWriter(_shouldAlignNumericValues, _shouldAlignNumericValues, _uniqueStringThresholdRatio,
                _strideLength, _bufferFactory, columnId);
        }
    }

    internal class ColumnWriterDetails
    {
        public string PropertyName { get; set; }
        public IColumnWriter ColumnWriter { get; set; }
        public Action<object> AddValueToState { get; set; }
        public Action WriteValuesFromState { get; set; }
        public ColumnStatistics FileStatistics { get; } = new ColumnStatistics();
        public ColumnType ColumnType { get; set; }
    }
}