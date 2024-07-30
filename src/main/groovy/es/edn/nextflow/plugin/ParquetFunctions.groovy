package es.edn.nextflow.plugin

import com.jerolba.carpet.CarpetReader
import com.jerolba.carpet.CarpetWriter
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.extension.CH
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.Session
import org.apache.parquet.hadoop.ParquetFileWriter

import java.nio.file.Path

@Slf4j
@CompileStatic
class ParquetFunctions extends PluginExtensionPoint{

    private Session session
    private PluginConfiguration configuration

    @Override
    protected void init(Session session) {
        this.session = session
        this.session.addShutdownHook {
            closeAllResources()
        }
        this.configuration = parseConfig(session.config.navigate('parquet') as Map)
    }

    protected PluginConfiguration parseConfig(Map map){
        new PluginConfiguration(map)
    }


    @Function
    String writeRecord(String filename, Record data){
        try (var outputStream = new FileOutputStream(filename)) {
            try (var writer = new CarpetWriter<>(outputStream, data.getClass() as Class<Record>)) {
                writer.write(data);
            }
        }
        return data.toString()
    }

    @Function
    String writeRecords(String filename, Collection<Record> data){
        Record first = data.first()
        try (var outputStream = new FileOutputStream(filename)) {
            try (var writer = new CarpetWriter<>(outputStream, first.getClass() as Class<Record>)) {
                writer.accept(first)
                writer.write(data.drop(1));
            }
        }
        return data.toString()
    }

    private Map<String, CarpetWriter> currentWriters = [:]

    @Function
    String createWriter(String filename, Object clazz){
        if(!(clazz instanceof Class<Record>) ){
            throw new IllegalArgumentException("A Record.class is required")
        }
        def currentOutputStream = new FileOutputStream(filename)
        def currentWriter = new CarpetWriter.Builder<>(currentOutputStream, clazz as Class<Record>)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()
        String id = "writer"+new Random().nextInt()
        currentWriters.put(id, currentWriter)
        id
    }

    @Function
    Record appendRecord(String id, Record data){
        if( currentWriters.containsKey(id)) {
            currentWriters[id].write(data)
        }
        data
    }

    @Function
    String endWriter(String id){
        if( currentWriters.containsKey(id)) {
            currentWriters[id].close()
            currentWriters.remove(id)
        }
        id
    }

    protected void closeAllResources(){
        currentWriters.keySet().each{
            endWriter(it)
        }
    }

    @Factory
    DataflowWriteChannel fromParquet(String path, Object clazz){
        if(!(clazz instanceof Class<Record>) ){
            throw new IllegalArgumentException("A Record.class is required")
        }
        final channel = CH.create()
        session.addIgniter((action) -> emitRawFile(channel, path, clazz as Class<Record>))
        return channel
    }

    private void emitRawFile(DataflowWriteChannel channel, String path, Class<Record> clazz) {
        try {
            log.info "Start reading $path, with projection $clazz"
            var reader = new CarpetReader(Path.of(path).toFile(), clazz)
            for(def record : reader){
                channel.bind(record)
            }
            log.info "Finished emitted $path, with projection $clazz"
            channel.bind(Channel.STOP)
        }catch (Throwable t){
            log.error("Error reading $path parquet file", t)
            session.abort(t)
        }
    }
}
