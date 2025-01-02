import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text productId = new Text();
    private Text record = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Expected schema: product_id, product_name, category, price, stock_quantity
        if (fields.length == 5) {
            try {
                // Validate product_id
                String id = isValidInteger(fields[0].trim()) ? fields[0].trim() : "";

                // Other fields
                String productName = fields[1].trim(); // No specific validation
                String category = fields[2].trim(); // No specific validation
                String priceStr = fields[3].trim();
                String stockStr = fields[4].trim();

                // Skip rows where product_id is empty
                if (id.isEmpty()) return;

                // Validate and transform price and stock_quantity
                int price = isValidInteger(priceStr) ? Integer.parseInt(priceStr) : 0;
                price = price < 0 ? Math.abs(price) : price; // Make positive if negative

                int stockQuantity = isValidInteger(stockStr) ? Integer.parseInt(stockStr) : 0;
                stockQuantity = stockQuantity < 0 ? Math.abs(stockQuantity) : stockQuantity; // Make positive if negative

                // Emit product_id as key and the rest of the record as value
                productId.set(id);
                record.set(productName + "," + category + "," + price + "," + stockQuantity);
                context.write(productId, record);

            } catch (Exception e) {
                // Ignore invalid rows
            }
        }
    }

    // Helper method to check if a string is a valid integer
    private boolean isValidInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
