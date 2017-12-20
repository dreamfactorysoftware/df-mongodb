<?php

namespace DreamFactory\Core\MongoDb\Models;
use DreamFactory\Core\File\Models\FilePublicPath;
/**
 * GridFSConfig
 *
 * @inheritdoc
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
        'public_path',
        'dsn',
        'options',
        'driver_options',
    ];

    public static function getConfig($id, $local_config = null, $protect = true)
    {
        $results = parent::getConfig($id, $local_config, $protect);
        /** @var FilePublicPath $pathConfig */
        if (!empty($pathConfig = FilePublicPath::find($id))) {
            $results = array_merge($results, $pathConfig->toArray());
        }
        return $results;
    }

    /**
     * {@inheritdoc}
     */
    public static function setConfig($id, $config, $local_config = null)
    {
        $results = parent::setConfig($id, $config, $local_config);
        if(!isset($config['container'])){
            $config['container'] = $config['database'];
        }
        $resultPath = FilePublicPath::setConfig($id, $config, $local_config);
        if ($resultPath) {
            $results = array_merge((array)$results, (array)$resultPath);
        }


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
        $pathSchema = (array)FilePublicPath::getConfigSchema();
        $schema = array_merge($schema, $pathSchema);
        // Allow upsert not relevant to gridfs
        unset($schema['allow_upsert']);

        // find a better order for bucket_name
        $reordered = [];
        foreach ($schema as $val) {
            $pos = array_search($val['name'], static::$sort);
            if ($pos !== false) {
                $reordered[$pos] = $val;
            }
        }
        ksort($reordered);

        return $reordered;
    }

    /**
     * @inheritdoc
     */
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