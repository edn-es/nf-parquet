include { createWriter; appendRecord} from 'plugin/nf-parquet'

import myrecords.*

def writerId = createWriter('append.parquet', CustomRecord)

channel.of( (0..10_000_000) )
        | map{
            appendRecord( writerId,
                    new CustomRecord(it, "test"+new Random().nextInt(), 10, new Random().nextDouble(), new Random().nextDouble()))
          }
        | count
        | view
