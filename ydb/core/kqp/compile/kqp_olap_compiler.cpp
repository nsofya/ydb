#include "kqp_olap_compiler.h"

#include <ydb/core/formats/arrow_helpers.h>
 
namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimrSSA;

constexpr ui32 OLAP_PROGRAM_VERSION = 1; 
 
namespace {

class TKqpOlapCompileContext {
public:
    TKqpOlapCompileContext(const TCoArgument& row, const TKikimrTableMetadata& tableMeta, 
        NKqpProto::TKqpPhyOpReadOlapRanges& readProto) 
        : Row(row)
        , MaxColumnId(0)
        , ReadProto(readProto) 
    {
        for (const auto& [_, columnMeta] : tableMeta.Columns) {
            YQL_ENSURE(ReadColumns.emplace(columnMeta.Name, columnMeta.Id).second);
            MaxColumnId = std::max(MaxColumnId, columnMeta.Id);
        }
 
        Program.SetVersion(OLAP_PROGRAM_VERSION); 
    }

    ui32 GetColumnId(const TStringBuf& name) const {
        auto column = ReadColumns.FindPtr(name);
        YQL_ENSURE(column);

        return *column;
    }

    ui32 NewColumnId() {
        return ++MaxColumnId;
    }

    const TExprNode* GetRowExpr() const {
        return Row.Raw();
    }

    TProgram::TAssignment* CreateAssignCmd() { 
        auto* cmd = Program.AddCommand(); 
        auto* assign = cmd->MutableAssign(); 
        assign->MutableColumn()->SetId(NewColumnId()); 
 
        return assign; 
    } 
 
    TProgram::TFilter* CreateFilter() { 
        return Program.AddCommand()->MutableFilter(); 
    } 
 
    void AddParameterName(const TString& name) { 
        ReadProto.AddOlapProgramParameterNames(name); 
    } 
 
    void SerializeToProto() { 
        TString programBytes; 
        TStringOutput stream(programBytes); 
        Program.SerializeToArcadiaStream(&stream); 
        ReadProto.SetOlapProgram(programBytes); 
    } 
 
private:
    TCoArgument Row;
    TMap<TString, ui32> ReadColumns;
    ui32 MaxColumnId;
    TProgram Program; 
    NKqpProto::TKqpPhyOpReadOlapRanges& ReadProto; 
};

 
TProgram::TAssignment* CompileCondition(const TExprBase& condition, TKqpOlapCompileContext& ctx); 

ui32 ConvertValueToColumn(const TCoDataCtor& value, TKqpOlapCompileContext& ctx) 
{ 
    TProgram::TAssignment* ssaValue = ctx.CreateAssignCmd(); 
 
    if (value.Maybe<TCoUtf8>()) { 
        auto nodeValue = value.Cast<TCoUtf8>().Literal().Value(); 
        ssaValue->MutableConstant()->SetText(TString(nodeValue)); 
    } else if (value.Maybe<TCoString>()) { 
        auto nodeValue = value.Cast<TCoString>().Literal().Value(); 
        ssaValue->MutableConstant()->SetText(TString(nodeValue)); 
    } else if (value.Maybe<TCoBool>()) { 
        auto nodeValue = value.Cast<TCoBool>().Literal().Value(); 
        ssaValue->MutableConstant()->SetBool(FromString<bool>(nodeValue)); 
    } else if (value.Maybe<TCoFloat>()) { 
        auto nodeValue = value.Cast<TCoFloat>().Literal().Value(); 
        ssaValue->MutableConstant()->SetFloat(FromString<float>(nodeValue)); 
    } else if (value.Maybe<TCoDouble>()) { 
        auto nodeValue = value.Cast<TCoDouble>().Literal().Value(); 
        ssaValue->MutableConstant()->SetDouble(FromString<double>(nodeValue)); 
    } else if (value.Maybe<TCoInt8>()) { 
        auto nodeValue = value.Cast<TCoInt8>().Literal().Value(); 
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue)); 
    } else if (value.Maybe<TCoInt16>()) { 
        auto nodeValue = value.Cast<TCoInt16>().Literal().Value(); 
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue)); 
    } else if (value.Maybe<TCoInt32>()) { 
        auto nodeValue = value.Cast<TCoInt32>().Literal().Value(); 
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue)); 
    } else if (value.Maybe<TCoInt64>()) { 
        auto nodeValue = value.Cast<TCoInt64>().Literal().Value(); 
        ssaValue->MutableConstant()->SetInt64(FromString<i64>(nodeValue)); 
    } else if (value.Maybe<TCoUint8>()) { 
        auto nodeValue = value.Cast<TCoUint8>().Literal().Value(); 
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue)); 
    } else if (value.Maybe<TCoUint16>()) { 
        auto nodeValue = value.Cast<TCoUint16>().Literal().Value(); 
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue)); 
    } else if (value.Maybe<TCoUint32>()) { 
        auto nodeValue = value.Cast<TCoUint32>().Literal().Value(); 
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue)); 
    } else if (value.Maybe<TCoUint64>()) { 
        auto nodeValue = value.Cast<TCoUint64>().Literal().Value(); 
        ssaValue->MutableConstant()->SetUint64(FromString<ui64>(nodeValue)); 
    } else { 
        YQL_ENSURE(false, "Unsupported content: " << value.Ptr()->Content()); 
    } 
 
    return ssaValue->GetColumn().GetId(); 
} 
 
ui32 ConvertParameterToColumn(const TCoParameter& parameter, TKqpOlapCompileContext& ctx) 
{ 
    TProgram::TAssignment* ssaValue = ctx.CreateAssignCmd(); 
 
    auto name = TString(parameter.Name().Value()); 
    auto maybeType = parameter.Type().Maybe<TCoDataType>(); 
 
    YQL_ENSURE(maybeType.IsValid(), "Unknown type content in conversion: " << parameter.Type().Ptr()->Content()); 
 
    auto newParameter = ssaValue->MutableParameter(); 
    newParameter->SetName(name); 
 
    ctx.AddParameterName(name); 
 
    return ssaValue->GetColumn().GetId(); 
} 
 
ui64 GetOrCreateColumnId(const TExprBase& node, TKqpOlapCompileContext& ctx) { 
    if (auto maybeData = node.Maybe<TCoDataCtor>()) { 
        return ConvertValueToColumn(maybeData.Cast(), ctx); 
    } 
 
    if (auto maybeAtom = node.Maybe<TCoAtom>()) { 
        return ctx.GetColumnId(maybeAtom.Cast().Value()); 
    } 
 
    if (auto maybeParameter = node.Maybe<TCoParameter>()) { 
        return ConvertParameterToColumn(maybeParameter.Cast(), ctx); 
    } 
 
    YQL_ENSURE(false, "Unknown node in OLAP comparison compiler: " << node.Ptr()->Content()); 
} 
 
TProgram::TAssignment* CompileComparison(const TKqpOlapFilterCompare& comparison, 
    TKqpOlapCompileContext& ctx)
{
    // Columns should be created before comparison, otherwise comparison fail to find columns 
    ui32 leftColumnId = GetOrCreateColumnId(comparison.Left(), ctx); 
    ui32 rightColumnId = GetOrCreateColumnId(comparison.Right(), ctx); 

    TProgram::TAssignment* command = ctx.CreateAssignCmd(); 
    auto* cmpFunc = command->MutableFunction(); 
 
    ui32 function = TProgram::TAssignment::FUNC_UNSPECIFIED; 
 
    if (comparison.Maybe<TKqpOlapFilterEqual>()) { 
        function = TProgram::TAssignment::FUNC_CMP_EQUAL; 
    } else if (comparison.Maybe<TKqpOlapFilterLess>()) { 
        function = TProgram::TAssignment::FUNC_CMP_LESS; 
    } else if (comparison.Maybe<TKqpOlapFilterLessOrEqual>()) { 
        function = TProgram::TAssignment::FUNC_CMP_LESS_EQUAL; 
    } else if (comparison.Maybe<TKqpOlapFilterGreater>()) { 
        function = TProgram::TAssignment::FUNC_CMP_GREATER; 
    } else if (comparison.Maybe<TKqpOlapFilterGreaterOrEqual>()) { 
        function = TProgram::TAssignment::FUNC_CMP_GREATER_EQUAL; 
    } 
 
    cmpFunc->SetId(function); 
    cmpFunc->AddArguments()->SetId(leftColumnId); 
    cmpFunc->AddArguments()->SetId(rightColumnId); 

    return command; 
}

TProgram::TAssignment* CompileExists(const TKqpOlapFilterExists& exists, 
    TKqpOlapCompileContext& ctx) 
{ 
    ui32 columnId = GetOrCreateColumnId(exists.Column(), ctx); 
 
    TProgram::TAssignment* command = ctx.CreateAssignCmd(); 
    auto* isNullFunc = command->MutableFunction(); 
 
    isNullFunc->SetId(TProgram::TAssignment::FUNC_IS_NULL); 
    isNullFunc->AddArguments()->SetId(columnId); 
 
    TProgram::TAssignment *notCommand = ctx.CreateAssignCmd(); 
    auto *notFunc = notCommand->MutableFunction(); 
 
    notFunc->SetId(TProgram::TAssignment::FUNC_BINARY_NOT); 
    notFunc->AddArguments()->SetId(command->GetColumn().GetId()); 
 
    return notCommand; 
} 
 
TProgram::TAssignment* BuildLogicalProgram(const TExprNode::TChildrenType& args, ui32 function, 
    TKqpOlapCompileContext& ctx)
{
    ui32 childrenCount = args.size(); 
 
    if (childrenCount == 1) { 
        // NOT operation is handled separately, thus only one available situation here: 
        // this is binary operation with only one node, just build this node and return. 
        return CompileCondition(TExprBase(args[0]), ctx); 
    } 
 
    TProgram::TAssignment* left = nullptr; 
    TProgram::TAssignment* right = nullptr; 
 
    if (childrenCount == 2) { 
        // Nice, we can build logical operation with two child as expected 
        left = CompileCondition(TExprBase(args[0]), ctx); 
        right = CompileCondition(TExprBase(args[1]), ctx); 
    } else { 
        // >2 children - split incoming vector in the middle call this function recursively. 
        auto leftArgs = args.Slice(0, childrenCount / 2); 
        auto rightArgs = args.Slice(childrenCount / 2); 
 
        left = BuildLogicalProgram(leftArgs, function, ctx); 
        right = BuildLogicalProgram(rightArgs, function, ctx); 
    } 
 
    TProgram::TAssignment *logicalOp = ctx.CreateAssignCmd(); 
    auto *logicalFunc = logicalOp->MutableFunction(); 
 
    logicalFunc->SetId(function); 
    logicalFunc->AddArguments()->SetId(left->GetColumn().GetId()); 
    logicalFunc->AddArguments()->SetId(right->GetColumn().GetId()); 
 
    return logicalOp; 
} 
 
TProgram::TAssignment* CompileCondition(const TExprBase& condition, TKqpOlapCompileContext& ctx) { 
    auto maybeCompare = condition.Maybe<TKqpOlapFilterCompare>(); 
 
    if (maybeCompare.IsValid()) { 
        return CompileComparison(maybeCompare.Cast(), ctx); 
    } 
 
    auto maybeExists = condition.Maybe<TKqpOlapFilterExists>(); 
 
    if (maybeExists.IsValid()) { 
        return CompileExists(maybeExists.Cast(), ctx); 
    } 
 
    if (auto maybeNot = condition.Maybe<TCoNot>()) { 
        // Not is a special way in case it has only one child 
        TProgram::TAssignment *value = CompileCondition(maybeNot.Cast().Value(), ctx); 
 
        TProgram::TAssignment *notOp = ctx.CreateAssignCmd(); 
        auto *notFunc = notOp->MutableFunction(); 
 
        notFunc->SetId(TProgram::TAssignment::FUNC_BINARY_NOT); 
        notFunc->AddArguments()->SetId(value->GetColumn().GetId()); 
 
        return notOp; 
    } 
 
    ui32 function = TProgram::TAssignment::FUNC_UNSPECIFIED; 
 
    if (condition.Maybe<TCoAnd>()) { 
        function = TProgram::TAssignment::FUNC_BINARY_AND; 
    } else if (condition.Maybe<TCoOr>()) { 
        function = TProgram::TAssignment::FUNC_BINARY_OR; 
    } else if (condition.Maybe<TCoXor>()) { 
        function = TProgram::TAssignment::FUNC_BINARY_XOR; 
    } else { 
        YQL_ENSURE(false, "Unsuppoted logical operation: " << condition.Ptr()->Content()); 
    } 
 
    return BuildLogicalProgram(condition.Ptr()->Children(), function, ctx); 
} 
 
void CompileFilter(const TKqpOlapFilter& filterNode, TKqpOlapCompileContext& ctx) { 
    TProgram::TAssignment* condition = CompileCondition(filterNode.Condition(), ctx); 
 
    auto* filter = ctx.CreateFilter(); 
    filter->MutablePredicate()->SetId(condition->GetColumn().GetId()); 
} 
 
void CompileOlapProgramImpl(TExprBase operation, TKqpOlapCompileContext& ctx) { 
    if (operation.Raw() == ctx.GetRowExpr()) {
        return;
    }

    if (auto maybeFilter = operation.Maybe<TKqpOlapFilter>()) { 
        CompileOlapProgramImpl(maybeFilter.Cast().Input(), ctx); 
        CompileFilter(maybeFilter.Cast(), ctx); 
        return;
    }

    YQL_ENSURE(operation.Maybe<TCallable>(), "Unexpected OLAP operation node type: " << operation.Ref().Type());
    YQL_ENSURE(false, "Unexpected OLAP operation: " << operation.Cast<TCallable>().CallableName());
}

} // namespace

void CompileOlapProgram(const TCoLambda& lambda, const TKikimrTableMetadata& tableMeta, 
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto) 
{ 
    YQL_ENSURE(lambda.Args().Size() == 1);

    TKqpOlapCompileContext ctx(lambda.Args().Arg(0), tableMeta, readProto); 

    CompileOlapProgramImpl(lambda.Body(), ctx); 
    ctx.SerializeToProto(); 
}

} // namespace NKqp
} // namespace NKikimr
