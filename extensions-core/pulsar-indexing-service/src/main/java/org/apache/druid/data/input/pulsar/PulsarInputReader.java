package org.apache.druid.data.input.pulsar;

import org.apache.druid.data.input.*;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.pulsar.client.api.schema.Field;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PulsarInputReader implements InputEntityReader {

    private final PulsarRecordEntity entity;

    public PulsarInputReader(InputRowSchema inputRowSchema, PulsarRecordEntity source) {
        this.entity = source;
    }

    @Override
    public CloseableIterator<InputRow> read() throws IOException {
        MapBasedInputRow inputRow = getMapBasedInputRow();
        return CloseableIterators.withEmptyBaggage(Collections.<InputRow>singleton(inputRow).iterator());
    }

    private MapBasedInputRow getMapBasedInputRow() {
        var msg = entity.getMessage();
        var timestamp = msg.getPublishTime();
        List<String> dimensions = msg.getValue().getFields().stream().map(Field::getName).collect(Collectors.toUnmodifiableList());
        Map<String, Object> events = msg.getValue().getFields().stream().collect(Collectors.toMap(Field::getName, field -> msg.getValue().getField(field)));
        MapBasedInputRow inputRow = new MapBasedInputRow(timestamp, dimensions, events);
        return inputRow;
    }

    @Override
    public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException {
        MapBasedInputRow inputRow = getMapBasedInputRow();
        var inputRowWithValues = InputRowListPlusRawValues.ofList(null, Collections.singletonList(inputRow));
        return CloseableIterators.withEmptyBaggage(Collections.singleton(inputRowWithValues).iterator());
    }
}
