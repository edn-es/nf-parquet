include { fromParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromParquet( "presidents.parquet", SinglePerson )
        | map { it.address().street() }
        | view