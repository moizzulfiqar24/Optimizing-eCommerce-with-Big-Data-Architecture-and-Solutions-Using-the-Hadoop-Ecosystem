import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;

public class CustomerHFileMapper extends Mapper<Object, Text, ImmutableBytesWritable, KeyValue> {

    private static final String COLUMN_FAMILY = "info";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: customer_id,name,email,phone,age,gender,location
        String[] fields = value.toString().split(",", -1); // Split with -1 to retain empty fields

        if (fields.length != 7) {
            // Log row with incorrect field count but process it with empty fields
            System.err.println("Incorrect field count: " + value.toString());
            fields = ensureFieldCount(fields, 7); // Pad or truncate fields to 7
        }

        // Trim whitespace from all fields
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
        }

        String customerId = fields[0];
        String name = fields[1];
        String email = fields[2];
        String phone = fields[3];
        String age = fields[4];
        String gender = fields[5];
        String location = fields[6];

        // Ensure customerId is present (rowKey cannot be null or empty)
        if (customerId == null || customerId.isEmpty()) {
            System.err.println("Missing customerId, skipping row: " + value.toString());
            return; // Skip this row
        }

        try {
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(customerId));

            // Write each field to HBase only if it is non-empty
            if (!name.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("name"), Bytes.toBytes(name)));
            }
            if (!email.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("email"), Bytes.toBytes(email)));
            }
            if (!phone.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("phone"), Bytes.toBytes(phone)));
            }
            if (!age.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("age"), Bytes.toBytes(age)));
            }
            if (!gender.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("gender"), Bytes.toBytes(gender)));
            }
            if (!location.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("location"), Bytes.toBytes(location)));
            }
        } catch (Exception e) {
            // Catch any unexpected issues and log the problematic row
            System.err.println("Error processing row: " + value.toString() + " | Error: " + e.getMessage());
        }
    }

    /**
     * Ensures the field array has exactly the expected number of fields.
     * Pads with empty strings or truncates as needed.
     */
    private String[] ensureFieldCount(String[] fields, int expectedCount) {
        String[] adjustedFields = new String[expectedCount];
        for (int i = 0; i < expectedCount; i++) {
            if (i < fields.length) {
                adjustedFields[i] = fields[i];
            } else {
                adjustedFields[i] = ""; // Pad with empty strings
            }
        }
        return adjustedFields;
    }
}
