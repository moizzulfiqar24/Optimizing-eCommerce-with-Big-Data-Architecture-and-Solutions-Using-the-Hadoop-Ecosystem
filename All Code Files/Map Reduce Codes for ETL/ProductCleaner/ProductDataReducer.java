import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductDataReducer extends Reducer<Text, Text, Text, Text> {
    private Text cleanedRecord = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String productName = fields[0]; // Retain as-is
            String category = fields[1]; // Retain as-is
            String priceStr = fields[2];
            String stockStr = fields[3];

            // Ensure price and stock_quantity are valid integers
            int price = isValidInteger(priceStr) ? Integer.parseInt(priceStr) : 0;
            price = price < 0 ? Math.abs(price) : price; // Make positive if negative

            int stockQuantity = isValidInteger(stockStr) ? Integer.parseInt(stockStr) : 0;
            stockQuantity = stockQuantity < 0 ? Math.abs(stockQuantity) : stockQuantity; // Make positive if negative

            // Write the cleaned record
            cleanedRecord.set(key.toString() + "," + productName + "," + category + "," + price + "," + stockQuantity);
            context.write(null, cleanedRecord); // `null` key for flat CSV output
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
