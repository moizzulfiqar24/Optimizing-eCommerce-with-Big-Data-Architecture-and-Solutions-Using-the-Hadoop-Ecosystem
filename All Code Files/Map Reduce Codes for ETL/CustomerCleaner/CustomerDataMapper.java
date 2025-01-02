import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomerDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text customerId = new Text();
    private Text record = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Expected schema: customer_id, name, email, phone, age, gender, location
        if (fields.length == 7) {
            try {
                // Validate customer_id
                String id = isValidInteger(fields[0].trim()) ? fields[0].trim() : "";

                // Other fields
                String name = fields[1].trim(); // No specific validation
                String email = fields[2].trim(); // No specific validation
                String phone = fields[3].trim(); // No specific validation for phone
                String age = isValidInteger(fields[4].trim()) ? fields[4].trim() : ""; // Validate age
                String gender = isValidChar(fields[5].trim()) ? fields[5].trim() : ""; // Validate gender
                String location = fields[6].trim(); // No specific validation

                // Skip rows where customer_id is empty
                if (id.isEmpty()) return;

                // Emit customer_id as key and the rest of the record as value
                customerId.set(id);
                record.set(name + "," + email + "," + phone + "," + age + "," + gender + "," + location);
                context.write(customerId, record);

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

    // Helper method to check if a string is a single character
    private boolean isValidChar(String value) {
        return value != null && value.length() == 1 && Character.isLetter(value.charAt(0));
    }
}
