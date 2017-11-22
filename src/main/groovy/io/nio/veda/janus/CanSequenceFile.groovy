package io.nio.veda.janus

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.DefaultCodec

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.hadoop.io.SequenceFile

class CanSequenceFile {
    static String timestamp2directory(Long src) {
        def format = DateTimeFormatter.ofPattern('yyyy/MM/dd/HH/mm/ss')
        def utc = ZonedDateTime.ofInstant(Instant.EPOCH.plusNanos(src), ZoneId.of("UTC"))
        return "${utc.format(format)}/${src}"
    }

    static def event2time(fs, src) {
        def itor = fs.listFiles(new Path(src), false)
        long start=Long.MAX_VALUE, end=0
        while(itor.hasNext()) {
            def afile = itor.next()
            if (afile.path.toString().endsWith('.txt')) {
                def listOfEvent = IOUtils.toString(fs.open(afile.path)).split('\n').collect {Long.parseLong(it)}
                def local_min = listOfEvent.min()
                def local_max = listOfEvent.max()
                if (start>local_min)
                    start = local_min
                if (local_max> end)
                    end = local_max
            }
        }
        return [start, end]
    }

    static String time2directory(String src) {
        //return Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'",src, TimeZone.getTimeZone('UTC'))
        def df = new SimpleDateFormat('yyyy/MM/dd/HH/mm/ss')
        def format =df.format(Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'",src))
        return format
    }

    static int diff(String src1, String src2) {
        if (src1.length() == src2.length()) {
            for(int i=0; i< src1.length(); i++) {
                if (src1.getAt(i) == src2.getAt(i))
                    continue
                return i
            }
        }
        return -1
    }

    static List filterOut(List fileList,String fs, String vehicle, String start, String end) {
        String prefix = "${fs}/data/hub/vehicle/${vehicle}"
        String startPath = "${prefix}/${start}"
        String endPath = "${prefix}/${end}"

        return fileList.findAll {
            def eventdir = it.split('/')[0..-4].join('/')
            startPath <= eventdir && eventdir <= endPath
        }
    }

    static String getFSPattern(String src1, String src2) {

        def diff_point = diff(src1,src2)
        if (diff_point > -1) {
            String prefix = "${src1[0..<diff_point]}[${src1[diff_point]}-${src2[diff_point]}]"
            if (diff_point < 4) {
                return "${prefix}*/*/*/*/*/*/*"
            } else if (diff_point < 7) {
                return "${prefix}*/*/*/*/*/*"
            } else if (diff_point < 10) {
                return "${prefix}*/*/*/*/*"
            } else if (diff_point < 13) {
                return "${prefix}*/*/*/*"
            } else if (diff_point < 16) {
                return "${prefix}*/*/*"
            } else if (diff_point < 19) {
                return "${prefix}*/*"
            } else {
                return "${prefix}*"
            }
        }
        return src1
    }

    static  SequenceFile.Writer getWriter(Configuration conf,FileSystem fs, String parentDir, String name) {
        Path filePath = new Path(parentDir, name)
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(filePath),
                SequenceFile.Writer.keyClass(LongWritable),
                SequenceFile.Writer.valueClass(Text),
                SequenceFile.Writer.bufferSize(fs.getConf().getInt('io.file.buffer.size', 4096)),
                SequenceFile.Writer.replication(fs.getDefaultReplication(filePath)),
                SequenceFile.Writer.blockSize(fs.getDefaultBlockSize(filePath)),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
                SequenceFile.Writer.progressable(null),
                SequenceFile.Writer.metadata(new SequenceFile.Metadata()))


        return writer
    }

    static void main(args) {

        def cli = new CliBuilder(usage: 'CanSequenceFile.groovy -[hf] -v [vehicle] -s [start event] -e [end event] -o [sequence file path]')

        cli.with {
            h (longOpt:'help', "help for this command")
            f (longOpt:'filesystem', args:1, optionalArg: true, 'hdfs file system hdfs://host:port')
            s (longOpt:'StartEvent', args:1, required: true, 'hdfs start event input directory')
            e (longOpt:'endEvent', args:1, required: true, 'hdfs end event input directory')
            v (longOpt:'vehicle', args:1, required: true, 'Vehicle name')
            o (longOpt:'output', args:1, required: true, 'output hdfs directory')
            t (longOpt:'thread', args:1, optionalArg: true, 'thread pool size')
        }

        def options = cli.parse(args)

        if (!options)
            return

        if (options.h ) {
            cli.usage()
            System.exit(1)
        }

        def hdfs='hdfs://10.118.31.8:9000'

        if (options.f) {
            hdfs = options.filesystem
        }

        def conf = new Configuration()
        conf.set('fs.defaultFS', hdfs)

        def fs = FileSystem.get(conf)

        //def (start_ts, end_ts) = event2time(fs, options.e)

        def start = timestamp2directory(Long.parseLong(options.s))
        def end = timestamp2directory(Long.parseLong(options.e))

        def fullPattern = "/data/hub/vehicle/${options.v}/${getFSPattern(start, end)}/can/mobileye_can/*.txt"
        println fullPattern

        def fileListBulk = fs.globStatus(new Path(fullPattern)).collect { it.path.toString() }

        def targetData = filterOut(fileListBulk, hdfs, options.v.toString(), start, end)


        SequenceFile.Writer write = getWriter(conf, fs, options.o.toString(), "CANwrite.seq")
        SequenceFile.Writer read = getWriter(conf, fs, options.o.toString(), "CANread.seq")

        try {
            targetData.each {
                println it
                def parts = it.split('/')
                String filename=parts[-1]
                String eventid=parts[-4]

                Path filePath = new Path(it)
                def status = fs.getFileStatus(filePath)
                byte [] buffer = new byte[status.getLen()]
                def fd = fs.open(filePath)
                fd.readFully(0, buffer)
                if (filename.matches(".*write1.txt")) {
                    write.append(new LongWritable(Long.parseLong(eventid)), new Text(buffer))
                } else if (filename.matches(".*read1.txt")) {
                    read.append(new LongWritable(Long.parseLong(eventid)), new Text(buffer))
                }
            }

        } finally {
            org.apache.hadoop.io.IOUtils.closeStream(write)
            org.apache.hadoop.io.IOUtils.closeStream(read)
        }

    }

}
