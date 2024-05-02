#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/datashard/operation.h>

namespace NKikimr::NColumnShard {

    class TEvWriteTransactionOperator : public TTxController::ITransactionOperator {
        using TBase = TTxController::ITransactionOperator;
        using TProposeResult = TTxController::TProposeResult;
        using TInReadSets = TMap<std::pair<ui64, ui64>, TVector<NDataShard::TRSData>>;
        static inline auto Registrator = TFactory::TRegistrator<TEvWriteTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE);
    public:
        using TBase::TBase;

        virtual bool Parse(TColumnShard& /*owner*/, const TString& data) override {
            NKikimrTxColumnShard::TCommitWriteTxBody commitTxBody;
            if (!commitTxBody.ParseFromString(data)) {
                return false;
            }
            LockId = commitTxBody.GetLockId();
            if (commitTxBody.HasKqpLocks()) {
                KqpLocks = commitTxBody.GetKqpLocks();
            }
            return !!LockId;
        }

        TProposeResult ExecuteOnPropose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const override;

        bool CompleteOnPropose(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) const override {
            return true;
        }

        virtual bool ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

        virtual bool CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) override {
            auto result = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(owner.TabletID(), GetTxId());
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
            return true;
        }

        virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            return owner.OperationsManager->AbortTransaction(owner, GetTxId(), txc);
        }
        virtual bool CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
            return true;
        }

    private:
        void SubscribeNewLocks(TColumnShard& owner, const TActorContext& ctx);

    private:
        ui64 LockId = 0;
        std::optional<NKikimrDataEvents::TKqpLocks> KqpLocks;
        TInReadSets InReadSets;
    };

}
