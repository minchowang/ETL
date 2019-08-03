import com.wmc.ETLUtils;

/**
 * @author: WangMC
 * @date: 2019/8/2 21:00
 * @description:
 */
public class Test {
    @org.junit.Test
    public void test1(){

        String string = "SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUks";
        System.out.println(ETLUtils.etlString(string));
    }
}
