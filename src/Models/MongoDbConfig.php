<?php
namespace DreamFactory\Core\MongoDb\Models;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use Illuminate\Database\Query\Builder;

/**
 * MongoDbConfig
 *
 * @property integer $service_id
 * @property string  $dsn
 * @property string  $options
 * @property string  $driver_options
 *
 * @method static Builder|MongoDbConfig whereServiceId($value)
 */
class MongoDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions;

    protected $table = 'mongodb_config';

    protected $fillable = ['service_id', 'dsn', 'options', 'driver_options'];

    protected $casts = ['options' => 'array', 'driver_options' => 'array'];

    public static function validateConfig($config, $create = true)
    {
        static::checkExtensions(['mongo']);

        if ((null === ArrayUtils::get($config, 'dsn', null, true))) {
            if ((null === ArrayUtils::getDeep($config, 'options', 'db', null, true))) {
                throw new BadRequestException('Database name must be included in the \'dsn\' or as an \'option\' attribute.');
            }
        }

        return true;
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
                $schema['default'] = 'mongodb://[username:password@]host1[:port1][,host2[:port2:],...]/db';
                $schema['description'] =
                    'The connection string for the service. The username, password, and db values can be added in the connection string or in the options below.' .
                    ' For further information, see http://php.net/manual/en/mongoclient.construct.php.';
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
                    ' For further options, see http://php.net/manual/en/mongoclient.construct.php.';
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