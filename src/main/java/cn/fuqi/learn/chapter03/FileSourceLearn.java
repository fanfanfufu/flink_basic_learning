package cn.fuqi.learn.chapter03;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author fanfanfufu
 * @version 1.0
 * @date 2023/3/26 17:25
 * @description 基于本地文件的source
 */
public class FileSourceLearn {
    public static void main(String[] args) throws Exception {
        // 1. first, create flink job execute environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2. second, get the file source
        // 2.1 old way to create file source
        DataStreamSource<String> fileSourceOld = env.readTextFile("src/main/resources/data/word.txt");
        fileSourceOld.name("old create file source method job").print();
        // 2.2 in version 1.16.0, flink suggest create file source by this way:
        Path filePath = new Path("src/main/resources/data/word.txt");
        FileSource<String> fileSourceNew = FileSource.forRecordStreamFormat(new TextLineInputFormat(), filePath).build();
        DataStreamSource<String> dataStreamSource = env.fromSource(fileSourceNew, WatermarkStrategy.noWatermarks(), "file-source-new");
        dataStreamSource.name("file source created by new way").print();

        env.execute("file source learn");
    }
}
