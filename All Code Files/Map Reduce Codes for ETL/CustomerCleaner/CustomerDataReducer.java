import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CustomerDataReducer extends Reducer<Text, Text, Text, Text> {
    private Text cleanedRecord = new Text();
    private ArrayList<Integer> ageList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalAge = 0;
        int validAgeCount = 0;
        String cleanedRow = "";

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String customerId = key.toString();
            String name = fields[0];
            String email = fields[1];
            String phone = fields[2];
            String ageStr = fields[3];
            String gender = fields[4];
            String location = fields[5];

            int age = ageStr.isEmpty() || Integer.parseInt(ageStr) < 0 ? -1 : Integer.parseInt(ageStr);

            // Collect valid ages for calculating the average
            if (age >= 0) {
                totalAge += age;
                validAgeCount++;
            }

            // Prepare the record with default values
            gender = gender.isEmpty() ? "O" : gender;
            location = location.isEmpty() ? "" : location;

            cleanedRow = customerId + "," + name + "," + email + "," + phone + "," + age + "," + gender + "," + location;
        }

        // Calculate the average age
        int averageAge = validAgeCount > 0 ? totalAge / validAgeCount : 0;

        // Adjust age field in the output
        String[] recordFields = cleanedRow.split(",");
        String customerId = recordFields[0];
        String phone = recordFields[3];
        int age = Integer.parseInt(recordFields[4]) < 0 ? averageAge : Integer.parseInt(recordFields[4]);

        // Write the cleaned record in CSV format
        cleanedRecord.set(customerId + "," + recordFields[1] + "," + recordFields[2] + "," + phone + "," + age + "," + recordFields[5] + "," + recordFields[6]);
        context.write(null, cleanedRecord); // `null` key for flat CSV output
    }
}
