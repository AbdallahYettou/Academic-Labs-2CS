package tp1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        // Loop through the values (1, 1, 1...) and add them up
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        result.set(sum);
        // Output: (Word, TotalCount)
        context.write(key, result);
    }
}