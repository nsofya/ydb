#include "tuple.h"
#include "hashes_calc.h"

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <util/generic/buffer.h>
#include <util/generic/bitops.h>

#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

    THolder<TTupleLayout> TTupleLayout::Create(const std::vector<TColumnDesc>& columns) {

        if (NX86::HaveAVX2())
            return MakeHolder<TTupleLayoutFallback<NSimd::TSimdAVX2Traits>>(columns);

        if (NX86::HaveSSE42())
            return MakeHolder<TTupleLayoutFallback<NSimd::TSimdSSE42Traits>>(columns);

        return MakeHolder<TTupleLayoutFallback<NSimd::TSimdFallbackTraits>>(columns);

    }

    template <typename TTraits>
    TTupleLayoutFallback<TTraits>::TTupleLayoutFallback(const std::vector<TColumnDesc>& columns) : TTupleLayout(columns) {

        for (ui32 i = 0, idx = 0; i < OrigColumns.size(); ++i) {
            auto &col = OrigColumns[i];

            col.OriginalIndex = idx;

            if (col.SizeType == EColumnSizeType::Variable) {
                // we cannot handle (rare) overflow strings unless we have at least space for header;
                // size of inlined strings is limited to 254 bytes, limit maximum inline data size
                col.DataSize = std::max<ui32>(1 + 2*sizeof(ui32), std::min<ui32>(255, col.DataSize));
                idx += 2; // Variable-size takes two columns: one for offsets, and another for payload
            } else {
                idx += 1;
            }

            if (col.Role == EColumnRole::Key) {
                KeyColumns.push_back(col);
            } else {
                PayloadColumns.push_back(col);
            }
        }

        KeyColumnsNum = KeyColumns.size();

        auto ColumnDescLess = [](const TColumnDesc& a, const TColumnDesc& b) {
            if (a.SizeType != b.SizeType) // Fixed first
                return a.SizeType == EColumnSizeType::Fixed;

            if (a.DataSize == b.DataSize)
                // relative order of (otherwise) same key columns must be preserved
                return a.OriginalIndex < b.OriginalIndex;

            return a.DataSize < b.DataSize;
        };

        std::sort(KeyColumns.begin(), KeyColumns.end(), ColumnDescLess);
        std::sort(PayloadColumns.begin(), PayloadColumns.end(), ColumnDescLess);

        KeyColumnsFixedEnd = 0;

        ui32 currOffset = 4; // crc32 hash in the beginning
        KeyColumnsOffset = currOffset;
        KeyColumnsFixedNum = KeyColumnsNum;

        for (ui32 i = 0; i < KeyColumnsNum; ++i) {
            auto &col = KeyColumns[i];

            if (col.SizeType == EColumnSizeType::Variable && KeyColumnsFixedEnd == 0) {
                KeyColumnsFixedEnd = currOffset;
                KeyColumnsFixedNum = i;
            }

            col.ColumnIndex = i;
            col.Offset = currOffset;
            Columns.push_back(col);
            currOffset += col.DataSize;
        }

        KeyColumnsEnd = currOffset;

        if (KeyColumnsFixedEnd == 0) // >= 4 if was ever assigned
            KeyColumnsFixedEnd = KeyColumnsEnd;

        KeyColumnsSize = KeyColumnsEnd - KeyColumnsOffset;
        BitmaskOffset = currOffset;

        BitmaskSize = (OrigColumns.size() + 7) / 8;

        currOffset += BitmaskSize;
        BitmaskEnd = currOffset;

        PayloadOffset = currOffset;

        for (ui32 i = 0; i < PayloadColumns.size(); ++i) {
            auto &col = PayloadColumns[i];
            col.ColumnIndex = KeyColumnsNum + i;
            col.Offset = currOffset;
            Columns.push_back(col);
            currOffset += col.DataSize;
        }

        PayloadEnd = currOffset;
        PayloadSize = PayloadEnd - PayloadOffset;

        TotalRowSize = currOffset;

        for (auto &col: Columns) {
            if (col.SizeType == EColumnSizeType::Variable) {
                VariableColumns_.push_back(col);
            } else if (IsPowerOf2(col.DataSize) && col.DataSize < (1u<<FixedPOTColumns_.size())) {
                FixedPOTColumns_[CountTrailingZeroBits(col.DataSize)].push_back(col);
            } else {
                FixedNPOTColumns_.push_back(col);
            }
        }
    }

    // Columns (SoA) format:
    //   for fixed size: packed data
    //   for variable size: offset (ui32) into next column; size of colum is rowCount + 1
    //
    // Row (AoS) format:
    //   fixed size: packed data
    //   variable size:
    //     assumes DataSize <= 255 && DataSize >= 1 + 2*4
    //     if size of payload is less than col.DataSize:
    //       u8 one byte of size (0..254)
    //       u8 [size] data
    //       u8 [DataSize - 1 - size] padding
    //     if size of payload is greater than DataSize:
    //       u8 = 255
    //       u32 = offset in overflow buffer
    //       u32 = size
    //       u8 [DataSize - 1 - 2*4] initial bytes of data
    // Data is expected to be consistent with isValidBitmask (0 for fixed-size, empty for variable-size)
    template <typename TTraits>
    void TTupleLayoutFallback<TTraits>::Pack( const ui8** columns, const ui8** isValidBitmask, ui8 * res, std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start, ui32 count) const {

        std::vector<ui64> bitmaskMatrix(BitmaskSize);

        for (; count--; ++start, res += TotalRowSize) {
            ui32 hash = 0;
            auto bitmaskIdx = start / 8;
            auto bitmaskShift = start % 8;

            bool anyOverflow = false;

            for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
                auto& col = KeyColumns[i];
                ui32 dataOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] + sizeof(ui32)*start);
                ui32 nextOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] + sizeof(ui32)*(start + 1));
                auto size = nextOffset - dataOffset;

                if (size >= col.DataSize) {
                    anyOverflow = true;
                    break;
                }
            }

            std::memset(res + BitmaskOffset, 0, BitmaskSize);

            for (ui32 i = 0; i < Columns.size(); ++i) {
                auto& col = Columns[i];

                res[BitmaskOffset + (i / 8)] |= ((isValidBitmask[col.OriginalIndex][bitmaskIdx] >> bitmaskShift) & 1u) << (i % 8);
            }

            for (auto &col: FixedNPOTColumns_) {
                std::memcpy(res + col.Offset, columns[col.OriginalIndex] + start*col.DataSize, col.DataSize);
            }

#define PackPOTColumn(POT) \
            for (auto &col: FixedPOTColumns_[POT]) { \
                std::memcpy(res + col.Offset, columns[col.OriginalIndex] + start*(1u<<POT), 1u<<POT); \
            }

            PackPOTColumn(0);
            PackPOTColumn(1);
            PackPOTColumn(2);
            PackPOTColumn(3);
            PackPOTColumn(4);
#undef PackPOTColumn

            for (auto &col: VariableColumns_) {
                    auto dataOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] + sizeof(ui32)*start);
                    auto nextOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] + sizeof(ui32)*(start + 1));
                    auto size = nextOffset - dataOffset;
                    auto data = columns[col.OriginalIndex + 1] + dataOffset;

                    if (size >= col.DataSize) {
                        res[col.Offset] = 255;

                        ui32 prefixSize = (col.DataSize - 1 - 2*sizeof(ui32));
                        auto overflowSize = size - prefixSize;
                        auto overflowOffset = overflow.size();

                        overflow.resize(overflowOffset + overflowSize);

                        WriteUnaligned<ui32>(res + col.Offset + 1 + 0*sizeof(ui32), overflowOffset);
                        WriteUnaligned<ui32>(res + col.Offset + 1 + 1*sizeof(ui32), overflowSize);
                        std::memcpy(res + col.Offset + 1 + 2*sizeof(ui32), data, prefixSize);
                        std::memcpy(overflow.data() + overflowOffset, data + prefixSize, overflowSize);
                    } else {
                        Y_DEBUG_ABORT_UNLESS(size < 255);
                        res[col.Offset] = size;
                        std::memcpy(res + col.Offset + 1, data, size);
                        std::memset(res + col.Offset + 1 + size, 0, col.DataSize - (size + 1));
                    }

                    if (anyOverflow && col.Role == EColumnRole::Key) {
                        hash = CalculateCRC32<TTraits, sizeof(ui32)>((ui8 *)&size, hash);
                        hash = CalculateCRC32<TTraits>(data, size, hash);
                    }
            }

            // isValid bitmap is NOT included into hashed data
            if (anyOverflow) {
                hash = CalculateCRC32<TTraits>(res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset, hash );
            } else {
                hash = CalculateCRC32<TTraits>(res + KeyColumnsOffset, KeyColumnsEnd - KeyColumnsOffset);
            }
            WriteUnaligned<ui32>(res, hash);
        }
    }
}
}
}
