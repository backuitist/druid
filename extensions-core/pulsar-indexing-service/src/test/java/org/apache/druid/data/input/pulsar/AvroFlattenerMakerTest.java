package org.apache.druid.data.input.pulsar;


import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.parsers.*;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class AvroFlattenerMakerTest {

    // The complex Avro schema provided by the user, formatted as a multi-line Java String.
    private static final String NORMALIZED_CHARGER_EVENT_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"NormalizedChargerEvent\",\"namespace\":\"hypervolt.analytics\",\"fields\":[" +
                    "{\"name\":\"deviceId\",\"type\":\"long\"}," +
                    "{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"placement\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"VersionedPlacementId\",\"namespace\":\"hypervolt.placement\",\"fields\":[" +
                    "{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"OPID\",\"namespace\":\"hypervolt.id\",\"fields\":[" +
                    "{\"name\":\"id\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}}," +
                    "{\"name\":\"version\",\"type\":\"int\"}]}]}," +
                    "{\"name\":\"payload\",\"type\":[" +
                    "{\"type\":\"record\",\"name\":\"Connection\",\"fields\":[" +
                    "{\"name\":\"status\",\"type\":{\"type\":\"enum\",\"name\":\"ConnectionStatus\",\"symbols\":[\"Connected\",\"Disconnected\"]}}]}," +
                    "{\"type\":\"record\",\"name\":\"Telemetry\",\"fields\":[" +
                    "{\"name\":\"rawV3\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"OriginalV3Telemetry\",\"fields\":[" +
                    "{\"name\":\"halId\",\"type\":\"int\"},{\"name\":\"emittedAt\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"data\",\"type\":\"bytes\"},{\"name\":\"replay\",\"type\":\"boolean\"},{\"name\":\"refresh\",\"type\":\"boolean\"}]}]}," +
                    "{\"name\":\"emittedAt\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"dfeReceivedAt\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"data\",\"type\":[" +
                    "{\"type\":\"record\",\"name\":\"BootCount\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EVCurrent\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EVVoltage\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EnergyFrom2ndCT\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EnergyFromEV\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EnergyFromGrid\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EnergyTo2ndCT\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EnergyToEV\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"EnergyToGrid\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"GridCurrent\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"GridVoltage\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"PilotStatus\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}," +
                    "{\"type\":\"record\",\"name\":\"SecondCTCurrent\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"SecondCTVoltage\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}," +
                    "{\"type\":\"record\",\"name\":\"WakeUpRoutine\",\"namespace\":\"hypervolt.analytics.TelemetryData\",\"fields\":[{\"name\":\"value\",\"type\":\"boolean\"}]}]}," +
                    "{\"name\":\"synthetic\",\"type\":\"boolean\"}," +
                    "{\"name\":\"maskedReason\",\"type\":[\"null\",\"string\"]}]}]}]," +
                    "\"__AVRO_READ_OFFSET__\":\"0\"}";

    @Test
    public void discoverRootFieldsShouldReturnNestedFields() {
        GenericData.Record record = makeRecord();
        AvroFlattenerMaker flattener = new AvroFlattenerMaker(true, true, true, true);
        Set<String> rootFields = flattener.discoverRootFields(record);
        Assert.assertEquals(Set.of("payload", "placement", "deviceId", "timestamp"), rootFields);
    }

    @Test
    public void flatten() {
        JSONPathSpec flattenSpec = new JSONPathSpec(
                null, // Pas de chemin initial nécessaire pour une spécification de haut niveau
                ImmutableList.of(
                        // Utilisation du constructeur JSONPathFieldSpec(type, name, expr) avec type PATH
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Connection.status", "$.payload.Connection.status"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.data.EnergyFromGrid.value", "$.payload.Telemetry.data.EnergyFromGrid.value"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.data.EnergyToEV.value", "$.payload.Telemetry.data.EnergyToEV.value"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.data.EnergyToGrid.value", "$.payload.Telemetry.data.EnergyToGrid.value"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.data.BootCount.value", "$.payload.Telemetry.data.BootCount.value"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.dfeReceivedAt", "$.payload.Telemetry.dfeReceivedAt"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.emittedAt", "$.payload.Telemetry.emittedAt"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.maskedReason", "$.payload.Telemetry.maskedReason"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.rawV3.data", "$.payload.Telemetry.rawV3.data"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.rawV3.emittedAt", "$.payload.Telemetry.rawV3.emittedAt"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.rawV3.halId", "$.payload.Telemetry.rawV3.halId"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.rawV3.refresh", "$.payload.Telemetry.rawV3.refresh"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.rawV3.replay", "$.payload.Telemetry.rawV3.replay"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "payload.Telemetry.synthetic", "$.payload.Telemetry.synthetic"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "placement.id.id", "$.placement.id.id"),
                        new JSONPathFieldSpec(JSONPathFieldType.PATH, "placement.version", "$.placement.version")
                )
        );

        ObjectFlattener flattener = ObjectFlatteners.create(flattenSpec, new AvroFlattenerMaker(true, true, true, true));
        Map<String, Object> flattened = flattener.flatten(makeRecord());
        for (var k : flattened.keySet()) {
            System.out.println(k + " = " + flattened.get(k));
        }
    }

    private static GenericData.Record makeRecord() {
        // 1. Parse the schema from the JSON string
        Schema mainSchema = new Schema.Parser().parse(NORMALIZED_CHARGER_EVENT_SCHEMA_JSON);
        System.out.println("Schema parsed successfully. Name: " + mainSchema.getName() + "\n");

        // Define a fixed timestamp in milliseconds
        long nowMillis = Instant.now().toEpochMilli();

        // --- 2. Build Nested Records (Innermost to Outermost) ---

        // A. Build OPID (hypervolt.id.OPID)
        Schema opidSchema = mainSchema.getField("placement").schema().getTypes().get(1) // VersionedPlacementId
                .getField("id").schema(); // OPID schema

        GenericRecord opidRecord = new GenericRecordBuilder(opidSchema)
                .set("id", UUID.randomUUID().toString()) // UUID logical type requires a string
                .build();

        // B. Build VersionedPlacementId (hypervolt.placement.VersionedPlacementId)
        Schema versionedPlacementIdSchema = mainSchema.getField("placement").schema().getTypes().get(1);
        GenericRecord versionedPlacementIdRecord = new GenericRecordBuilder(versionedPlacementIdSchema)
                .set("id", opidRecord)
                .set("version", 1)
                .build();

        // C. Build TelemetryData Record (hypervolt.analytics.TelemetryData.BootCount)
        // We need to find the specific record schema within the union type of 'data'
        Schema telemetryDataUnion = mainSchema.getField("payload").schema().getTypes().get(1) // Telemetry schema
                .getField("data").schema(); // Union of TelemetryData records

        // Find the BootCount schema (it's the first in the union list)
        Schema bootCountSchema = telemetryDataUnion.getTypes().stream()
                .filter(s -> s.getName().equals("BootCount"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("BootCount schema not found."));

        GenericRecord bootCountRecord = new GenericRecordBuilder(bootCountSchema)
                .set("value", 10L) // Example boot count
                .build();

        // D. Build Telemetry Record (Telemetry)
        Schema telemetrySchema = mainSchema.getField("payload").schema().getTypes().get(1); // Telemetry schema

        // rawV3 is a Union ["null", OriginalV3Telemetry]. We set it to null.
        // maskedReason is a Union ["null", "string"]. We set it to null.
        GenericRecord telemetryRecord = new GenericRecordBuilder(telemetrySchema)
                .set("rawV3", null) // Set to null (first type in union)
                .set("emittedAt", nowMillis)
                .set("dfeReceivedAt", nowMillis + 500) // 500ms later
                .set("data", bootCountRecord) // Place the BootCount record into the 'data' union
                .set("synthetic", false)
                .set("maskedReason", null) // Set to null (first type in union)
                .build();

        // --- 3. Build the Main Record (NormalizedChargerEvent) ---
        return new GenericRecordBuilder(mainSchema)
                .set("deviceId", 98765L)
                .set("timestamp", nowMillis)
                // Set the VersionedPlacementId record into the 'placement' union (second type)
                .set("placement", versionedPlacementIdRecord)
                // Set the Telemetry record into the 'payload' union (second type)
                .set("payload", telemetryRecord)
                .build();
    }

}