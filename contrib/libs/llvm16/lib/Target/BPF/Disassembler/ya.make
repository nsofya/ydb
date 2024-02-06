# Generated by devtools/yamaker.

LIBRARY()

LICENSE(Apache-2.0 WITH LLVM-exception)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/llvm16
    contrib/libs/llvm16/include
    contrib/libs/llvm16/lib/MC
    contrib/libs/llvm16/lib/MC/MCDisassembler
    contrib/libs/llvm16/lib/Support
    contrib/libs/llvm16/lib/Target/BPF/TargetInfo
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/llvm16/lib/Target/BPF
    contrib/libs/llvm16/lib/Target/BPF
    contrib/libs/llvm16/lib/Target/BPF/Disassembler
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    BPFDisassembler.cpp
)

END()
