package es.edn.nextflow.plugin

import groovy.transform.PackageScope

@PackageScope
class PluginConfiguration {

    final AwsConfig  awsConfig

    PluginConfiguration(Map map){
        if( map?.containsKey('aws') ){
            awsConfig = new AwsConfig(
                    endpoint: map.navigate('aws.client.endpoint'),
                    region: map.navigate('aws.region'),
                    accessKey: map.navigate('aws.accessKey'),
                    secretKey: map.navigate('aws.secretKey'),
            )
        }else{
            awsConfig = null
        }
    }

    static class AwsConfig{
        String endpoint
        String accessKey
        String secretKey
        String region
    }
}
