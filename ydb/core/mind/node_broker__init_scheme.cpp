#include "node_broker_impl.h" 
#include "node_broker__scheme.h" 
 
namespace NKikimr { 
namespace NNodeBroker { 
 
class TNodeBroker::TTxInitScheme : public TTransactionBase<TNodeBroker> { 
public: 
    TTxInitScheme(TNodeBroker *self) 
        : TBase(self) 
    { 
    } 
 
    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override 
    { 
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxInitScheme Execute"); 
 
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>(); 
 
        return true; 
    } 
 
    void Complete(const TActorContext &ctx) override 
    { 
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxInitScheme Complete"); 
 
        Self->ProcessTx(Self->CreateTxLoadState(), ctx); 
        Self->TxCompleted(this, ctx); 
    } 
}; 
 
ITransaction *TNodeBroker::CreateTxInitScheme() 
{ 
    return new TTxInitScheme(this); 
} 
 
} // NNodeBroker 
} // NKikimr 
