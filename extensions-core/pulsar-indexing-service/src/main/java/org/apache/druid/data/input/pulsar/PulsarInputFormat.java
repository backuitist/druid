package org.apache.druid.data.input.pulsar;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.File;

public class PulsarInputFormat extends NestedInputFormat {

    protected PulsarInputFormat(
            @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec) {
        super(flattenSpec);
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory) {
        return new PulsarInputReader(inputRowSchema, (PulsarRecordEntity)source, getFlattenSpec());
    }
}
