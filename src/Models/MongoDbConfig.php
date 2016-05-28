<?php
namespace DreamFactory\Core\MongoDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Models\ServiceCacheConfig;
use DreamFactory\Core\MongoDb\Services\MongoDb;

/**
 * MongoDbConfig
 *
 * @property integer $service_id
 * @property string  $dsn
 * @property array   $options
 * @property array   $driver_options
 *
 * @method static MongoDbConfig whereServiceId($value)
 */
class MongoDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions;

    protected $table = 'mongodb_config';

    protected $fillable = ['service_id', 'dsn', 'options', 'driver_options'];

    protected $casts = [
        'service_id'     => 'integer',
        'options'        => 'array',
        'driver_options' => 'array'
    ];
    
    /**
     * @param int $id
     *
     * @return array
     */
    public static function getConfig($id)
    {
        $config = parent::getConfig($id);

        $cacheConfig = ServiceCacheConfig::whereServiceId($id)->first();
        $config['cache_enabled'] = (empty($cacheConfig)) ? false : $cacheConfig->getAttribute('cache_enabled');
        $config['cache_ttl'] = (empty($cacheConfig)) ? 0 : $cacheConfig->getAttribute('cache_ttl');

        return $config;
    }

    public static function validateConfig($config, $create = true)
    {
        static::checkExtensions(['mongodb']);

        if (empty(array_get($config, 'options.db')) && empty(array_get($config, 'options.database'))) {
            //  Attempt to find db in connection string
            $dsn = strval(array_get($config, 'dsn'));
            $db = strstr(substr($dsn, MongoDb::DSN_PREFIX_LENGTH), '/');
            if (false !== $pos = strpos($db, '?')) {
                $db = substr($db, 0, $pos);
            }
            $db = trim($db, '/');
            if (empty($db)) {
                throw new BadRequestException("Database name must be included in the dsn or provided as an 'option' attribute.");
            }
        }


        return true;
    }

    /**
     * {@inheritdoc}
     */
    public static function setConfig($id, $config)
    {
        $cache = [];
        if (isset($config['cache_enabled'])) {
            $cache['cache_enabled'] = $config['cache_enabled'];
            unset($config['cache_enabled']);
        }
        if (isset($config['cache_ttl'])) {
            $cache['cache_ttl'] = $config['cache_ttl'];
            unset($config['cache_ttl']);
        }
        if (!empty($cache)) {
            ServiceCacheConfig::setConfig($id, $cache);
        }

        parent::setConfig($id, $config);
    }

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'dsn':
                $schema['label'] = 'Connection String';
                $schema['default'] = 'mongodb://[username:password@]host1[:port1][,host2[:port2:],...][/database][?options]';
                $schema['description'] =
                    'The connection string for the service, i.e. mongodb://[username:password@]host1[:port1][,host2[:port2:],...][/database][?options]. ' .
                    ' The username, password, and database values can be added in the connection string or in the options below.' .
                    ' For further information, see https://docs.mongodb.com/manual/reference/connection-string/#connection-string-options.';
                break;
            case 'options':
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] =
                    'An array of options for the connection.' .
                    ' For further options, see https://docs.mongodb.com/manual/reference/connection-string/#connection-string-options.';
                break;
            case 'driver_options':
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] =
                    'An array of options for the MongoDB driver, currently just supporting "context".' .
                    ' For further information, see http://php.net/manual/en/mongo.connecting.ssl.php#mongo.connecting.context.ssl.';
                break;
        }
    }
}