package NaivePerformanceTest;

import java.io.Serializable;

public class Record implements Serializable {
    String continent;
    String country;
    String itemType;
    String buyMethod;
    String initial;
    String startDate;
    long number1;
    String endDate;
    int number2;
    float number3;
    float number4;
    float number5;
    float number6;
    float number7;

    public Record(String continent, String country, String itemType, String buyMethod, String initial, String startDate,
                  long number1, String endDate, int number2, float number3, float number4, float number5, float number6, float number7){
        this.continent = continent;
        this.country = country;
        this.itemType = itemType;
        this.buyMethod = buyMethod;
        this.initial = initial;
        this.startDate = startDate;
        this.number1 = number1;
        this.endDate = endDate;
        this.number2 = number2;
        this.number3 = number3;
        this.number4 = number4;
        this.number5 = number5;
        this.number6 = number6;
        this.number7 = number7;
    }
}
