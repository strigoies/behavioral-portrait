package com.yisa.utils;

import com.yisa.BehavioralPortrait;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

@Slf4j
public class ReadConfig {
    public static ConfigEntity getConfigEntity() {
        ConfigEntity configEntity;
        String filePath = "config.yaml";
        try {
            Yaml yaml = new Yaml();
            Options options = new Options();
            options.addOption("c", "config", true, "config file path");
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, BehavioralPortrait.args);

            if (cmd.hasOption("c")) {
                // 通过命令行参数查找配置文件
                filePath = cmd.getOptionValue("c");
            }
            InputStream inputStream = new FileInputStream(filePath);
            configEntity = yaml.loadAs(inputStream, ConfigEntity.class);

        } catch (ParseException | FileNotFoundException e) {
            log.error("错误指定启动参数！");
            throw new RuntimeException(e);
        }
        return configEntity;
    }
}
