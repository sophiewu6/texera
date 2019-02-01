package pause;


import java.io.BufferedReader;
import java.io.Serializable;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

interface abstr extends Serializable { }

public class Operator implements abstr{

  public static final String BACKEND_REGISTRATION = "BackendRegistration";
  public static boolean PAUSE = false;
  public ArrayList<String[]> text = new ArrayList<>();



  public static void turnTrue(){
    PAUSE = true;
  }

  public static void turnFalse(){
    PAUSE = false;
  }
}

 class Join extends Operator {
    private static final long serialVersionUID = 1L;

    public int port;

    public Join(String port) {
      this.port = Integer.parseInt(port);
    }
  }

 class Scan extends Operator {

    private static final long serialVersionUID = 1L;
    private static int counter = 0;
    public static BufferedReader br;
    public static boolean eof = false;

    public Scan(String path) {
      try {
        br = new BufferedReader(new FileReader(path));
      }catch (Exception e) {
        e.printStackTrace();
      }
    }

    public ArrayList<String[]> getSourceObj(int batch) {
      ArrayList<String[]> buff = new ArrayList<>();
      String input = "";
      try{
        while (counter < batch && (input = br.readLine()) != null)
        {
          String[] line = input.split(",");
          buff.add(line);
          System.out.println(Arrays.toString(line));
          counter++;
        }
        if (br.readLine() == null)
          eof = true;
        counter = 0;
      }catch (Exception e) {
        e.printStackTrace();
      }
//      System.out.println("Scan finish\nPrepare to search");
      return buff;
    }
  }

 class Matcher extends Operator {

    private static final long serialVersionUID = 1L;
    private ArrayList<String[]> source;

    public Matcher(List<String[]> source) {
      this.source = new ArrayList<>(source);
    }

    public ArrayList<String[]> getText(){
      for (String[] line : source)
      {
//        System.out.println(Arrays.toString(line));
        if (line[0].equals("Asia"))
        {
          System.out.println("keyword found");
          text.add(line);
        }
        else
          System.out.println("keyword not found");
      }
      return this.text;
    }
  }

  class Sink extends Operator {
    private static final long serialVersionUID = 1L;

    public Sink(List<String[]> text) {
      this.text = new ArrayList<>(text);
    }

    public void getText(){
      for (String[] line: text)
        System.out.println(line[0] +", "+ line[1] +", "+ line[2]);
    }
  }