include { fromParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromParquet( params.file, SingleRecord )
        | view