import com.yisa.utils.ConfigEntity;
import org.junit.Test;

import static com.yisa.utils.ReadConfig.getConfigEntity;

public class ConfigTest {
    @Test
    public void configTest(){
        ConfigEntity configEntity = getConfigEntity();
        System.out.println(configEntity);

    }
}
