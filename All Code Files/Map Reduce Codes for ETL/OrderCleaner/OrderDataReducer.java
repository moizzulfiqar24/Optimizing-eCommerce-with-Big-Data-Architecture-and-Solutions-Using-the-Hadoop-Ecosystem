import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderDataReducer extends Reducer<Text, Text, Text, Text> {
    private Text cleanedRecord = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String customerId = fields[0];
            String productId = fields[1];
            int quantity = Integer.parseInt(fields[2]);
            int totalAmount = Integer.parseInt(fields[3]);
            String transactionDate = fields[4];
            String paymentMethod = fields[5];

            // Write the cleaned record in CSV format
            cleanedRecord.set(key.toString() + "," + customerId + "," + productId + "," + quantity + "," + totalAmount + "," + transactionDate + "," + paymentMethod);
            context.write(null, cleanedRecord); // `null` key for flat CSV output
        }
    }
}
