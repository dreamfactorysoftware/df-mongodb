<?php

namespace DreamFactory\Core\MongoDb\Models;

use DreamFactory\Core\MongoDb\Models\MongoDbConfig;

/**
 * MongoDbConfig
 *
 * @property integer $service_id
 * @property string  $dsn
 * @property string  $host
 * @property integer $port
 * @property string  $database
 * @property string  $username
 * @property string  $password
 * @property array   $options
 * @property array   $driver_options
 *
 */
class GridFsConfig extends MongoDbConfig
{

    protected static $sort = [
        'host',
        'port',
        'database',
        'username',
        'password',
        'bucket_name',
        'dsn',
        'options',
        'driver_options',
        'max_records',
        'cache_enabled',
        'cache_ttl',
    ];

    public static function getConfig($id, $local_config = null, $protect = true)
    {
        $results = parent::getConfig($id, $local_config, $protect);

        return $results;
    }

    /**
     * {@inheritdoc}
     */
    public static function setConfig($id, $config, $local_config = null)
    {
        $results = parent::setConfig($id, $config, $local_config);
        if (isset($config['bucket_name'])) {
            $results['bucket_name'] = $config['bucket_name'];
        }

        unset($results['allow_upsert']);

        return $results;
    }

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $schema = (array)parent::getConfigSchema();
        unset($schema['allow_upsert']);

        $reordered = [];
        foreach($schema as $val){
            $reordered[array_search($val['name'], static::$sort)] = $val;
        }
        ksort($reordered);
        return $reordered;
    }

    public static function getExtraConfigSchema()
    {
        $return = parent::getExtraConfigSchema();

        $extra = [
            [
                'name'        => 'bucket_name',
                'label'       => 'GridFS Bucket Name',
                'type'        => 'string',
                'allow_null'  => true,
                'default'     => null,
                'required'    => false,
                'description' => 'Name of the GridFS Bucket. (Optional).',
            ]
        ];

        return array_merge($return, $extra);
    }

}