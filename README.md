# Orcneas
Read and Extract from ORC files for U-SQL

# Building
Build in Visual Studio 2017.

# Usage
After building, deploy the assemblies to a folder (in the example below, "Orcneas"), and reference the necessary assemblies, then use the usual pipeline.
````
DROP ASSEMBLY IF EXISTS [System.ValueTuple];
DROP ASSEMBLY IF EXISTS [protobuf-net];
DROP ASSEMBLY IF EXISTS [ApacheOrcDotNet];
DROP ASSEMBLY IF EXISTS [Orcneas.Core];

CREATE ASSEMBLY [System.ValueTuple] FROM @"\Orcneas\System.ValueTuple.dll";
CREATE ASSEMBLY [protobuf-net] FROM @"\Orcneas\protobuf-net.dll";
CREATE ASSEMBLY [ApacheOrcDotNet] FROM @"\Orcneas\ApacheOrcDotNet.dll";
CREATE ASSEMBLY [Orcneas.Core] FROM @"\Orcneas\Orcneas.Core.dll";

REFERENCE ASSEMBLY [System.ValueTuple];
REFERENCE ASSEMBLY [protobuf-net];
REFERENCE ASSEMBLY [ApacheOrcDotNet];
REFERENCE ASSEMBLY [Orcneas.Core];

@Result = 
    EXTRACT
        title string,
        publish_date DateTime?,
        item_value double
    FROM
        @"\Orc\Orcneas.orc" 
    USING new Orcneas.Core.OrcExtractor();

OUTPUT @Result TO "/output/test/Orcneas.Csv" USING Outputters.Csv();
````
# LICENSE
See LICENSE. 
