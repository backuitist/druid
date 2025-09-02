package org.apache.druid.data.input.pulsar;

import com.google.common.collect.Iterators;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.*;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PulsarInputReader extends IntermediateRowParsingReader<GenericRecord> {

    private final InputRowSchema inputRowSchema;
    private final PulsarRecordEntity source;
    private final ObjectFlattener<GenericRecord> recordFlattener;

    public PulsarInputReader(InputRowSchema inputRowSchema, PulsarRecordEntity source, JSONPathSpec flattenSpec) {
        this.inputRowSchema = inputRowSchema;
        this.source = source;
        this.recordFlattener = ObjectFlatteners.create(
                flattenSpec,
                new AvroFlattenerMaker(
                        false,
                        false,
                        true,
                        inputRowSchema.getDimensionsSpec().useSchemaDiscovery()
                )
        );
    }

    @Override
    protected CloseableIterator<GenericRecord> intermediateRowIterator() throws IOException
    {
        Object nativeObject = source.getMessage().getValue().getNativeObject();
        if (nativeObject instanceof GenericRecord) {
            return CloseableIterators.withEmptyBaggage(
                    Iterators.singletonIterator((GenericRecord) nativeObject));
        } else throw new IOException("Not an Avro generic record: " + nativeObject);
    }

    @Override
    protected List<InputRow> parseInputRows(GenericRecord intermediateRow) throws ParseException
    {
        return Collections.singletonList(
                MapInputRowParser.parse(
                        inputRowSchema,
                        recordFlattener.flatten(intermediateRow)
                )
        );
    }

    @Override
    protected List<Map<String, Object>> toMap(GenericRecord intermediateRow)
    {
        return Collections.singletonList(recordFlattener.toMap(intermediateRow));
    }
}
