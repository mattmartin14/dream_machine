import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;

// to compile: javac JavaWriteSpeed
// to run: java JavaWriteSpeed
public class JavaWriteSpeed {
    public static void main(String[] args) {

        String file_nm = System.getProperty("user.home")+"/test_dummy_data/write_benchmark/java_test.csv";

        int tot_rows = 1000000000;
        int buffer_size = 1024 * 1024 * 10;
        int batch_size = 100000;

        long start_ts = System.currentTimeMillis();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file_nm), buffer_size)) {
            for (int i = 1; i <= tot_rows; i++) {
                writer.write(Integer.toString(i)); 
                writer.newLine(); 

                if (i % batch_size == 0) {
                    writer.flush();
                }

            }

            writer.flush();

            long end_ts = System.currentTimeMillis();

            long elapsed_ml = end_ts - start_ts;

            double elapsed_sec = (double) elapsed_ml / 1000.0;


            NumberFormat numberFormat = NumberFormat.getNumberInstance();
            String fmt_tot_rows = numberFormat.format(tot_rows);
            
            System.out.println("Successfully wrote " + fmt_tot_rows + " rows in " + elapsed_sec + " seconds");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}