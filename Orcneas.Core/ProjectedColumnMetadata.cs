using ApacheOrcDotNet.Protocol;
using Microsoft.Analytics.Interfaces;

namespace Orcneas.Core
{
    //Used to map from the EXTRACT expression's column definition list to a orc sourced column.
    public sealed class ProjectedColumnMetadata
    {
        public ColumnTypeKind ColumnTypeKind { get; set; }
        public uint OrcColumnIndex { get; set; }
        public string OrcColumnName { get; set; }
        public int USqlProjectionColumnIndex { get; set; }
        public IColumn USqlProjectionColumn { get; set; }
    }
}