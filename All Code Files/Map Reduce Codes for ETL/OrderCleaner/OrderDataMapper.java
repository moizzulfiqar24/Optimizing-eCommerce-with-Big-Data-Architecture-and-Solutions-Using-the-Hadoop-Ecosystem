import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;

public class OrderDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text orderId = new Text();
    private Text record = new Text();
    private HashMap<String, Integer> productPrices = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String productDataPath = conf.get("product.data.path");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(productDataPath);

        // Load product prices from HDFS into a HashMap
        if (fs.exists(path)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                String productId = fields[0].trim();
                int price = Integer.parseInt(fields[3].trim()); // Assuming the 4th column is the price
                productPrices.put(productId, price);
            }
            br.close();
        } else {
            System.err.println("Product data file not found: " + productDataPath);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Expected schema: order_id, customer_id, product_id, quantity, total_amount, transaction_date, payment_method
        if (fields.length == 7) {
            try {
                // Validate fields
                String orderIdStr = isValidInteger(fields[0].trim()) ? fields[0].trim() : "";
                String customerIdStr = isValidInteger(fields[1].trim()) ? fields[1].trim() : "";
                String productId = fields[2].trim();
                String quantityStr = fields[3].trim();
                String totalAmountStr = fields[4].trim();
                String transactionDate = fields[5].trim();
                String paymentMethod = fields[6].trim();

                // Skip rows where order_id is empty
                if (orderIdStr.isEmpty()) return;

                int quantity = isValidInteger(quantityStr) ? Integer.parseInt(quantityStr) : 0;
                int totalAmount = isValidInteger(totalAmountStr) ? Integer.parseInt(totalAmountStr) : 0;

                // Lookup price using product_id
                int price = productPrices.getOrDefault(productId, 1);

                // Adjust quantity if it is <0 or =0
                if (quantity < 0) {
                    quantity = Math.abs(quantity); // Make quantity positive
                } else if (quantity == 0) {
                    if (totalAmount > 0) {
                        quantity = (int) Math.ceil((double) totalAmount / price);
                    } else {
                        quantity = 1;
                    }
                }

                // Adjust total_amount if it is <0 or =0
                if (totalAmount < 0) {
                    totalAmount = Math.abs(totalAmount); // Make total_amount positive
                } else if (totalAmount == 0) {
                    totalAmount = quantity * price;
                }

                // Adjust payment_method if it is empty
                if (paymentMethod.isEmpty()) {
                    paymentMethod = "Cash";
                }

                // Emit order_id as key and the rest of the record as value
                orderId.set(orderIdStr);
                record.set(customerIdStr + "," + productId + "," + quantity + "," + totalAmount + "," + transactionDate + "," + paymentMethod);
                context.write(orderId, record);

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
