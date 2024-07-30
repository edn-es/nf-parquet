include {fromParquet; writeRecords} from 'plugin/nf-parquet'

import myrecords.*

writeRecords("test2.parquet", [
        new CustomRecord(1,"test1",11, new Random().nextDouble(), new Random().nextDouble()),
        new CustomRecord(2,"test2",12, new Random().nextDouble(), new Random().nextDouble()),
        new CustomRecord(3,"test3",13, new Random().nextDouble(), new Random().nextDouble()),
])

process HELLO {
    input: val(customer)
    output: stdout
    """
    echo hello ${customer.name()}
    """
}

workflow {
    channel.fromParquet("test2.parquet", CustomRecord)
            | HELLO
            | view
}