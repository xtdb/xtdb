package xtdb.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import xtdb.api.TransactionKey;
import xtdb.query.*;
import xtdb.tx.*;
import xtdb.tx.Call;

import static xtdb.query.Query.*;

public class XtdbMapper {
    public static final SimpleModule TX_DESERIALIZER_MODULE = new SimpleModule("TxDeserializerModule")
            .addDeserializer(Ops.class, new OpsDeserializer())
            .addDeserializer(Put.class, new PutDeserializer())
            .addDeserializer(Delete.class, new DeleteDeserializer())
            .addDeserializer(Erase.class, new EraseDeserializer())
            .addDeserializer(Call.class, new CallDeserializer())
            .addDeserializer(Tx.class, new TxDeserializer())
            .addDeserializer(TxOptions.class, new TxOptionsDeserializer());

    public static final ObjectMapper TX_OP_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new JsonLdModule())
            .registerModule(TX_DESERIALIZER_MODULE);

    public static final SimpleModule QUERY_REQUEST_DESERIALIZER = new SimpleModule("QueryRequestDeserializer")
            .addDeserializer(QueryRequest.class, new QueryRequestDeserializer())
            .addDeserializer(Query.class, new QueryDeserializer())
            .addDeserializer(QueryOpts.class, new QueryOptsDeserializer())
            .addDeserializer(QueryTail.class, new QueryTailDeserializer())
            .addDeserializer(Unify.class, new UnifyDeserializer())
            .addDeserializer(UnifyClause.class, new UnifyClauseDeserializer())
            .addDeserializer(Pipeline.class, new PipelineDeserializer())
            .addDeserializer(From.class, new FromDeserializer())
            .addDeserializer(Where.class, new WhereDeserializer())
            .addDeserializer(Limit.class, new LimitDeserializer())
            .addDeserializer(Offset.class, new OffsetDeserializer())
            .addDeserializer(OrderBy.class, new OrderByDeserializer())
            .addDeserializer(Return.class, new ReturnDeserializer())
            .addDeserializer(UnnestCol.class, new UnnestColDeserializer())
            .addDeserializer(UnnestVar.class, new UnnestVarDeserializer())
            .addDeserializer(With.class, new WithDeserializer())
            .addDeserializer(WithCols.class, new WithColsDeserializer())
            .addDeserializer(Without.class, new WithoutDeserializer())
            .addDeserializer(IJoin.class, new IJoinDeserializer())
            .addDeserializer(Aggregate.class, new AggregateDeserializer())
            .addDeserializer(Relation.class, new RelDeserializer())
            .addDeserializer(VarSpec.class, new VarSpecDeserializer())
            .addDeserializer(Binding.class, new BindingDeserializer())
            .addDeserializer(TransactionKey.class, new TxKeyDeserializer())
            .addDeserializer(Basis.class, new BasisDeserializer())
            .addDeserializer(Expr.class, new ExprDeserializer())
            .addDeserializer(TemporalFilter.class, new TemporalFilterDeserializer());

    public static final ObjectMapper QUERY_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new JsonLdModule())
            .registerModule(QUERY_REQUEST_DESERIALIZER);
}
