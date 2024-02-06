# Generated by devtools/yamaker.

PROGRAM()

LICENSE(Apache-2.0 WITH LLVM-exception)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/llvm16
    contrib/libs/llvm16/include
    contrib/libs/llvm16/lib/Analysis
    contrib/libs/llvm16/lib/AsmParser
    contrib/libs/llvm16/lib/BinaryFormat
    contrib/libs/llvm16/lib/Bitcode/Reader
    contrib/libs/llvm16/lib/Bitcode/Writer
    contrib/libs/llvm16/lib/Bitstream/Reader
    contrib/libs/llvm16/lib/CodeGen
    contrib/libs/llvm16/lib/DebugInfo/CodeView
    contrib/libs/llvm16/lib/DebugInfo/DWARF
    contrib/libs/llvm16/lib/DebugInfo/MSF
    contrib/libs/llvm16/lib/DebugInfo/PDB
    contrib/libs/llvm16/lib/DebugInfo/Symbolize
    contrib/libs/llvm16/lib/Demangle
    contrib/libs/llvm16/lib/Frontend/OpenMP
    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/IRReader
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/MC
    contrib/libs/llvm16/lib/MC/MCDisassembler
    contrib/libs/llvm16/lib/MC/MCParser
    contrib/libs/llvm16/lib/Object
    contrib/libs/llvm16/lib/ProfileData
    contrib/libs/llvm16/lib/Remarks
    contrib/libs/llvm16/lib/Support
    contrib/libs/llvm16/lib/Target
    contrib/libs/llvm16/lib/Target/AArch64/Disassembler
    contrib/libs/llvm16/lib/Target/AArch64/MCTargetDesc
    contrib/libs/llvm16/lib/Target/AArch64/TargetInfo
    contrib/libs/llvm16/lib/Target/AArch64/Utils
    contrib/libs/llvm16/lib/Target/ARM/Disassembler
    contrib/libs/llvm16/lib/Target/ARM/MCTargetDesc
    contrib/libs/llvm16/lib/Target/ARM/TargetInfo
    contrib/libs/llvm16/lib/Target/ARM/Utils
    contrib/libs/llvm16/lib/Target/BPF/Disassembler
    contrib/libs/llvm16/lib/Target/BPF/MCTargetDesc
    contrib/libs/llvm16/lib/Target/BPF/TargetInfo
    contrib/libs/llvm16/lib/Target/LoongArch/Disassembler
    contrib/libs/llvm16/lib/Target/LoongArch/MCTargetDesc
    contrib/libs/llvm16/lib/Target/LoongArch/TargetInfo
    contrib/libs/llvm16/lib/Target/NVPTX/MCTargetDesc
    contrib/libs/llvm16/lib/Target/NVPTX/TargetInfo
    contrib/libs/llvm16/lib/Target/PowerPC/Disassembler
    contrib/libs/llvm16/lib/Target/PowerPC/MCTargetDesc
    contrib/libs/llvm16/lib/Target/PowerPC/TargetInfo
    contrib/libs/llvm16/lib/Target/WebAssembly/Disassembler
    contrib/libs/llvm16/lib/Target/WebAssembly/MCTargetDesc
    contrib/libs/llvm16/lib/Target/WebAssembly/TargetInfo
    contrib/libs/llvm16/lib/Target/WebAssembly/Utils
    contrib/libs/llvm16/lib/Target/X86/Disassembler
    contrib/libs/llvm16/lib/Target/X86/MCTargetDesc
    contrib/libs/llvm16/lib/Target/X86/TargetInfo
    contrib/libs/llvm16/lib/TargetParser
    contrib/libs/llvm16/lib/TextAPI
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Transforms/InstCombine
    contrib/libs/llvm16/lib/Transforms/Instrumentation
    contrib/libs/llvm16/lib/Transforms/ObjCARC
    contrib/libs/llvm16/lib/Transforms/Scalar
    contrib/libs/llvm16/lib/Transforms/Utils
    contrib/libs/llvm16/lib/Transforms/Vectorize
)

ADDINCL(
    contrib/libs/llvm16/tools/llvm-profgen
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    CSPreInliner.cpp
    MissingFrameInferrer.cpp
    PerfReader.cpp
    ProfileGenerator.cpp
    ProfiledBinary.cpp
    llvm-profgen.cpp
)

END()
