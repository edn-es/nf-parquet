include { createWriter; appendRecord; endWriter} from 'plugin/nf-parquet'

import myrecords.*

createWriter(params.file, CustomRecord)

channel.of( (0..1_000_000) )
        | map{
            appendRecord(
                    new CustomRecord(it, "test"+new Random().nextInt(), 10, new Random().nextDouble(), new Random().nextDouble())
            )
          }
        | count
        | view
