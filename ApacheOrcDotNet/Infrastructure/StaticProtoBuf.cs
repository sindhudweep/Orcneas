﻿using ProtoBuf.Meta;

namespace ApacheOrcDotNet.Infrastructure
{
    public static class StaticProtoBuf
    {
        static StaticProtoBuf()
        {
            Serializer = TypeModel.Create();
            Serializer.UseImplicitZeroDefaults = false;
        }

        public static RuntimeTypeModel Serializer { get; }
    }
}