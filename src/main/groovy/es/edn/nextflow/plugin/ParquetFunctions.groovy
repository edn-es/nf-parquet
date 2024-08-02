package es.edn.nextflow.plugin

import com.jerolba.carpet.CarpetReader
import com.jerolba.carpet.CarpetWriter
import com.jerolba.carpet.io.OutputStreamOutputFile
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.extension.CH
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.Session
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile

import java.nio.file.Path

@Slf4j
@CompileStatic
class ParquetFunctions extends PluginExtensionPoint{

    private Session session
    private PluginConfiguration configuration

    @Override
    protected void init(Session session) {
        this.session = session
        this.session.onShutdown {
            closeAllResources()
        }
        this.configuration = parseConfig(session.config)
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
    String createWriter(String filename, Object clazz) {
        if (!(clazz instanceof Class<Record>)) {
            throw new IllegalArgumentException("A Record.class is required")
        }

        def currentOutputStream = filename.startsWith("s3:") ?
                createS3Writer(filename, clazz) :
                createLocalWriter(filename, clazz)

        def currentWriter = new CarpetWriter.Builder<>(currentOutputStream, clazz as Class<Record>)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()
        String id = "writer"+new Random().nextInt()
        currentWriters.put(id, currentWriter)
        id
    }

    OutputFile createLocalWriter(String filename, Object clazz){
        def currentOutputStream = new FileOutputStream(filename)
        return new OutputStreamOutputFile(currentOutputStream)
    }

    OutputFile createS3Writer(String filename, Object clazz){
        Configuration config = newConfiguration()
        final String s3a = filename.replace("s3:","s3a:")
        org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(s3a)

        def currentOutputStream = HadoopOutputFile.fromPath(hPath, config)
        return currentOutputStream
    }

    @Function
    Record appendRecord(Record data, Map options=[:]){
        String id = options.id ?: currentWriters?.entrySet()?.first()?.key
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
    DataflowWriteChannel fromParquet(Object objPath, Object clazz){
        if(!(clazz instanceof Class<Record>) ){
            throw new IllegalArgumentException("A Record.class is required")
        }
        Path path = null
        if( objPath instanceof Path){
            path = objPath as Path
        }
        if( objPath instanceof CharSequence){
            path = Path.of(objPath.toString())
        }
        if(!path){
            throw new IllegalArgumentException("$objPath can't be converted to Path")
        }
        final channel = CH.create()
        session.addIgniter((action) -> emitRawFile(channel, path, clazz as Class<Record>))
        return channel
    }

    @Factory
    DataflowWriteChannel fromRawParquet(Object objPath) {
        Path path = null
        if( objPath instanceof Path){
            path = objPath as Path
        }
        if( objPath instanceof CharSequence){
            path = Path.of(objPath.toString())
        }
        if(!path){
            throw new IllegalArgumentException("$objPath can't be converted to Path")
        }
        final channel = CH.create()
        session.addIgniter((action) -> emitRawFile(channel, path, Map))
        return channel
    }

    private void emitRawFile(DataflowWriteChannel channel, Path path, Class clazz) {
        try {
            log.info "Start reading $path, with projection $clazz"
            if( path.toString().startsWith("s3:")){
                emitS3File(channel, path, clazz)
            }else {
                emitLocalFile(channel, path, clazz)
            }
            log.info "Finished emitted $path, with projection $clazz"
            channel.bind(Channel.STOP)
        }catch (Throwable t){
            log.error("Error reading $path parquet file", t)
            session.abort(t)
        }
    }


    private void emitLocalFile(DataflowWriteChannel channel, Path path, Class clazz) {
        var reader = new CarpetReader(path.toFile(), clazz)
        for (def record : reader) {
            channel.bind(record)
        }
    }

    private void emitS3File(DataflowWriteChannel channel, Path path, Class clazz) {
        Configuration config = newConfiguration()
        final String s3a = path.toString().replace("s3:/","s3a://") //double slash required
        org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(s3a)
        var inputFile = HadoopInputFile.fromPath(hPath, config)
        var reader = new CarpetReader(inputFile, clazz)
        for (def record : reader) {
            channel.bind(record)
        }
    }

    private Configuration newConfiguration(){
        Configuration config = new Configuration()
        config.classLoader = ParquetFunctions.classLoader
        config.set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem")
        if (configuration?.awsConfig?.accessKey)
            config.set("fs.s3a.access.key", configuration?.awsConfig?.accessKey)
        else
            config.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        if (configuration?.awsConfig?.secretKey)
            config.set("fs.s3a.secret.key", configuration?.awsConfig?.secretKey)
        if (configuration?.awsConfig?.endpoint)
            config.set("fs.s3a.endpoint", configuration?.awsConfig?.endpoint)
        if (configuration?.awsConfig?.region)
            config.set("fs.s3a.endpoint.region", configuration?.awsConfig?.region)
        config
    }

}
