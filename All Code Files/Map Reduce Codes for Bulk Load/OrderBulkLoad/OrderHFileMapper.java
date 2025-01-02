import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;

public class OrderHFileMapper extends Mapper<Object, Text, ImmutableBytesWritable, KeyValue> {

    private static final String COLUMN_FAMILY = "info";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: order_id,customer_id,product_id,quantity,total_amount,transaction_date,payment_method
        String[] fields = value.toString().split(",", -1); // Retain empty fields with -1

        if (fields.length != 7) {
            // Log row with incorrect field count but process it with empty fields
            System.err.println("Incorrect field count: " + value.toString());
            fields = ensureFieldCount(fields, 7); // Pad or truncate fields to 7
        }

        // Trim whitespace from all fields
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
        }

        String orderId = fields[0];
        String customerId = fields[1];
        String productId = fields[2];
        String quantity = fields[3];
        String totalAmount = fields[4];
        String transactionDate = fields[5];
        String paymentMethod = fields[6];

        // Ensure orderId is present (rowKey cannot be null or empty)
        if (orderId == null || orderId.isEmpty()) {
            System.err.println("Missing orderId, skipping row: " + value.toString());
            return; // Skip this row
        }

        try {
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(orderId));

            // Write each field to HBase only if it is non-empty
            if (!customerId.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("customer_id"), Bytes.toBytes(customerId)));
            }
            if (!productId.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("product_id"), Bytes.toBytes(productId)));
            }
            if (!quantity.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("quantity"), Bytes.toBytes(quantity)));
            }
            if (!totalAmount.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_amount"), Bytes.toBytes(totalAmount)));
            }
            if (!transactionDate.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("transaction_date"), Bytes.toBytes(transactionDate)));
            }
            if (!paymentMethod.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("payment_method"), Bytes.toBytes(paymentMethod)));
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
