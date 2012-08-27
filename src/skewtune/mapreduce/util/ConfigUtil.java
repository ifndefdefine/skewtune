package skewtune.mapreduce.util;

import org.apache.hadoop.conf.Configuration;

public class ConfigUtil {
    public static void loadResources() {
        Configuration.addDefaultResource("skewtune-default.xml");
        Configuration.addDefaultResource("skewtune-site.xml");
    }
}
