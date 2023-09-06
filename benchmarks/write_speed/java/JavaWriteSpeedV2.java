import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;

// to compile: javac JavaWriteSpeed
// to run: java JavaWriteSpeed
public class JavaWriteSpeedV2 {
    public static void main(String[] args) {

        String file_nm = System.getProperty("user.home")+"/test_dummy_data/write_benchmark/java_test.csv";

        int tot_rows = 1000000000;
        //int tot_rows = 1000;
        int buffer_size = 1024 * 1024 * 10;
        int batch_size = 100000;
        //int batch_size = 100;

        long start_ts = System.currentTimeMillis();

        // this version is roughly half a second slower than the one using modulus for batching...go figure
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file_nm), buffer_size)) {

            // batch operations
            for(int i = 1; i <= tot_rows; i+=batch_size) {

                int batch_start = i;
                int batch_end = batch_start+batch_size-1;
                
                //System.out.println("batch start is: " + batch_start + "; batch end is "+batch_end+"");

                for(int j = batch_start; j<=batch_end;j++) {
                    writer.write(Integer.toString(j)); 
                    writer.newLine();
                }
         
                writer.flush();

            }

            long end_ts = System.currentTimeMillis();

            long elapsed_ml = end_ts - start_ts;

            double elapsed_sec = (double) elapsed_ml / 1000.0;


            NumberFormat numberFormat = NumberFormat.getNumberInstance();
            String fmt_tot_rows = numberFormat.format(tot_rows);
            
            System.out.println("Java Test Harness: Successfully wrote " + fmt_tot_rows + " rows in " + elapsed_sec + " seconds");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}