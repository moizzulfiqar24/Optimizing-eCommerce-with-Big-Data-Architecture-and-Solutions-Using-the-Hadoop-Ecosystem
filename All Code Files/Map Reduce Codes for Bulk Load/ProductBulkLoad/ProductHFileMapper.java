import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;

public class ProductHFileMapper extends Mapper<Object, Text, ImmutableBytesWritable, KeyValue> {

    private static final String DETAILS_COLUMN_FAMILY = "details";
    private static final String INVENTORY_COLUMN_FAMILY = "inventory";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: product_id,product_name,category,price,stock_quantity
        String[] fields = value.toString().split(",", -1); // Retain empty fields with -1

        if (fields.length != 5) {
            // Log row with incorrect field count but process it with empty fields
            System.err.println("Incorrect field count: " + value.toString());
            fields = ensureFieldCount(fields, 5); // Pad or truncate fields to 5
        }

        // Trim whitespace from all fields
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
        }

        String productId = fields[0];
        String productName = fields[1];
        String category = fields[2];
        String price = fields[3];
        String stockQuantity = fields[4];

        // Ensure productId is present (rowKey cannot be null or empty)
        if (productId == null || productId.isEmpty()) {
            System.err.println("Missing productId, skipping row: " + value.toString());
            return; // Skip this row
        }

        try {
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(productId));

            // Write each field to HBase only if it is non-empty
            if (!productName.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(DETAILS_COLUMN_FAMILY), Bytes.toBytes("product_name"), Bytes.toBytes(productName)));
            }
            if (!category.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(DETAILS_COLUMN_FAMILY), Bytes.toBytes("category"), Bytes.toBytes(category)));
            }
            if (!price.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(INVENTORY_COLUMN_FAMILY), Bytes.toBytes("price"), Bytes.toBytes(price)));
            }
            if (!stockQuantity.isEmpty()) {
                context.write(rowKey, new KeyValue(rowKey.get(), Bytes.toBytes(INVENTORY_COLUMN_FAMILY), Bytes.toBytes("stock_quantity"), Bytes.toBytes(stockQuantity)));
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
