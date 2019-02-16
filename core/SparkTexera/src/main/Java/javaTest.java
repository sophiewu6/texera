import java.io.*;

public class javaTest {

    public static void main(String[] arg){

        long start = System.currentTimeMillis();

        File file = new File("/Users/yuranyan/Downloads/datasets/very_super_large_input.csv");
        BufferedReader reader = null;
        int count = 0;

        try{
            reader = new BufferedReader(new FileReader(file));
            String text;
            while ((text = reader.readLine()) != null) {
//                String[] row = text.split(",");
//                if(row[0].contains("Africa")){
//                    count++;
//                }
            }

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {

            }
        }

        System.out.println();
        System.out.println(count);

        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }
}
