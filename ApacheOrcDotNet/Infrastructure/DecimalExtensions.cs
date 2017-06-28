﻿using System;

namespace ApacheOrcDotNet.Infrastructure
{
    public static class DecimalExtensions
    {
        private static readonly long[] _scaleFactors =
        {
            1L,
            10L,
            100L,
            1000L,
            10000L,
            100000L,
            1000000L,
            10000000L,
            100000000L,
            1000000000L,
            10000000000L,
            100000000000L,
            1000000000000L,
            10000000000000L,
            100000000000000L,
            1000000000000000L,
            10000000000000000L,
            100000000000000000L,
            1000000000000000000L
        };

        public static Tuple<long, byte> ToLongAndScale(this decimal value)
        {
            var bits = decimal.GetBits(value);
            if (bits[2] != 0 || (bits[1] & 0x80000000) != 0)
                throw new OverflowException("Attempted to convert a decimal with greater than 63 bits of precision to a long");
            var m = (long)bits[0] | (long)(bits[1] << 32);
            var e = (byte)((bits[3] >> 16) & 0x7F);
            var isNeg = (bits[3] & 0x80000000) != 0;
            if (isNeg)
                m = -m;
            return Tuple.Create(m, e);
        }

        public static decimal ToDecimal(this Tuple<long, byte> value)
        {
            var m = value.Item1;
            var e = value.Item2;
            var isNeg = m < 0;
            if (isNeg)
                m = -m;
            return new decimal((int) m, (int) (m >> 32), 0, isNeg, e);
        }

        public static Tuple<long, byte> Rescale(this Tuple<long, byte> value, int desiredScale,
            bool truncateIfNecessary)
        {
            var m = value.Item1;
            var e = value.Item2;

            if (e == desiredScale)
            {
                return value;
            }
            if (desiredScale > e)
            {
                var scaleAdjustment = desiredScale - e;
                checked
                {
                    //Throw if we overflow a long here
                    var newM = m * _scaleFactors[scaleAdjustment];
                    var newE = (byte) (e + scaleAdjustment);
                    return Tuple.Create(newM, newE);
                }
            }
            else
            {
                var scaleAdjustment = e - desiredScale;
                var newM = m / _scaleFactors[scaleAdjustment];
                var newE = (byte) (e - scaleAdjustment);
                if (!truncateIfNecessary)
                    if (newM * _scaleFactors[scaleAdjustment] != m) //We lost information in the scaling
                        throw new ArithmeticException("Scaling would have rounded");
                return Tuple.Create(newM, newE);
            }
        }
    }
}