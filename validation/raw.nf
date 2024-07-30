include {
    fromRawParquet
} from 'plugin/nf-parquet'

channel.fromRawParquet( "presidents.parquet" )
        | map { it.address.street }
        | view