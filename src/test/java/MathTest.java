import com.yisa.model.CalSta;
import org.junit.Test;

public class MathTest {
    @Test
    public void mathTest() {
        CalSta cal = new CalSta();
//        double[] testdata = {2, 4, 6, 7, 8, 9, 12, 36};
        double[] testdata = {2,2,2,3,2,1,1,2};
        System.out.println("总和Sum  " + cal.Sum(testdata));
        System.out.println("平均值Mean  " + cal.Mean(testdata));
        System.out.println("总体方差Population Variance  " + cal.POP_Variance(testdata));
        System.out.println("总体标准差Population STD_dev   " + cal.POP_STD_dev(testdata));
        System.out.println("样本方差Sample Variance  " + cal.Sample_Variance(testdata));
        System.out.println("样本标准差Sample STD_dev   " + cal.Sample_STD_dev(testdata));
    }
}
