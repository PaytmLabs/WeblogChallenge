package web.log.challenge.pig;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.io.IOException;
import java.util.UUID;

/**
 * Identifies sessions in log entries, previously grouped by user key
 */
public class MarkSessions extends AccumulatorEvalFunc<DataBag> {
    private final int idleMilliseconds;
    private DataBag sessions;

    public MarkSessions(String idleSeconds) {
        this.idleMilliseconds = Integer.valueOf(idleSeconds) * 1000;
    }

    @Override
    public void accumulate(Tuple input) throws IOException {
        sessions = BagFactory.getInstance().newDefaultBag();

        String session_id = null;
        DateTime start_time = null;
        DateTime expire_time = null;

        final DataBag values = (DataBag)input.get(0);
        for (final Tuple hit : values) {
            final DateTime access_time = new DateTime(hit.get(0));
            if (expire_time == null || access_time.isAfter(expire_time)) { // first hit OR new session
                session_id = UUID.randomUUID().toString();
                start_time = access_time;
            }
            expire_time = access_time.plus(idleMilliseconds);
            final Integer duration = new Period(start_time, access_time).getSeconds();

            final Tuple enhanced = TupleFactory.getInstance().newTuple(hit.getAll());
            enhanced.append(session_id);
            enhanced.append(duration);

            sessions.add(enhanced);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        final Schema schema;
        try {
            // BAG.TUPLE.schema
            schema = input.getField(0).schema.getField(0).schema;
        } catch (FrontendException e) {
            throw new RuntimeException("Can't parse input structure", e);
        }

        final Schema enhancedSchema;
        try {
            enhancedSchema = schema.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Can't clone input structure", e);
        }

        enhancedSchema.add( new Schema.FieldSchema("session_id", DataType.CHARARRAY));
        enhancedSchema.add(new Schema.FieldSchema("session_duration", DataType.INTEGER));

        return new Schema(new Schema.FieldSchema(getSchemaName(getClass().getName().toLowerCase(), schema), DataType.BAG));
    }

    @Override
    public DataBag getValue() {
        return sessions;
    }

    @Override
    public void cleanup() {
        sessions = null;
    }
}

