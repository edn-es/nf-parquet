include {
    fromRawParquet
} from 'plugin/nf-parquet'

channel.fromRawParquet( params.file )
        | view