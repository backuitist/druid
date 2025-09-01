package org.apache.druid.data.input.pulsar;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;

import java.io.File;

public class PulsarInputFormat implements InputFormat {

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory) {
        return new PulsarInputReader(inputRowSchema, (PulsarRecordEntity)source);
    }
}
