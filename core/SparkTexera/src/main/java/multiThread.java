
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class multiThread {

    public static void main(String[] args){

        long start = System.currentTimeMillis();
        int rowNumber = 100000000;

        File file = new File("/home/yuran/Downloads/Twelve_g_input.csv");
        BufferedReader reader = null;
        List<String> fileContent = new ArrayList<>();

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while ((text = reader.readLine()) != null) {
                String[] row = text.split(",");
                fileContent.add(row[0]);
            }

            List<Callable<Integer>> threads = new ArrayList<>();
            threads.add(new TestThread(0,rowNumber / 5, fileContent, "Asia"));
            threads.add(new TestThread(rowNumber / 5,rowNumber * 2 / 5, fileContent, "Asia"));
            threads.add(new TestThread(rowNumber * 2 / 5,rowNumber * 3 / 5, fileContent, "Asia"));
            threads.add(new TestThread(rowNumber * 3 / 5,rowNumber * 4 / 5, fileContent, "Asia"));
            threads.add(new TestThread(rowNumber * 4 / 5, rowNumber, fileContent, "Asia"));

            ExecutorService exec = Executors.newFixedThreadPool(5);
            List<Future<Integer>> results = exec.invokeAll(threads);

            int counter = 0;
            for(Future f: results) {
                counter += (int)f.get();
            }
            System.out.println();
            System.out.println(counter);
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
        catch (ExecutionException e){
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


        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }
}

// User defined threads
class TestThread implements Callable<Integer> {

    private int localCount;
    private int startIndex;
    private int endIndex;
    private List<String> fileContent;
    private String keyWord;

    public TestThread(int startIndex, int endIndex, List<String> fileContent, String keyWord){
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.fileContent = fileContent;
        this.keyWord = keyWord;
    }

    @Override
    public Integer call() {
        for(int i = startIndex; i < endIndex; i++){
            if(fileContent.get(i).contains(keyWord)){
                localCount++;
            }
        }
        return localCount;
    }

}
