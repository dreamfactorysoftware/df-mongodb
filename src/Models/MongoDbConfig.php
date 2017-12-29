<?php
namespace DreamFactory\Core\MongoDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Components\SupportsExtraDbConfigs;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\MongoDb\Services\MongoDb;

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
class MongoDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions, SupportsExtraDbConfigs;

    protected $table = 'mongodb_config';

    protected $fillable = [
        'service_id',
        'dsn',
        'host',
        'port',
        'database',
        'username',
        'password',
        'options',
        'driver_options'
    ];

    protected $casts = [
        'service_id'     => 'integer',
        'port'           => 'integer',
        'options'        => 'array',
        'driver_options' => 'array'
    ];

    protected $encrypted = ['username', 'password'];

    protected $protected = ['password'];

    /**
     * {@inheritdoc}
     */
    public function validate($data, $throwException = true)
    {
        static::checkExtensions(['mongodb']);

        if (empty(array_get($data, 'database')) && empty(array_get($data, 'options.db')) &&
            empty(array_get($data, 'options.database'))
        ) {
            //  Attempt to find db in connection string
            $dsn = strval(array_get($data, 'dsn'));
            $db = strstr(substr($dsn, MongoDb::DSN_PREFIX_LENGTH), '/');
            if (false !== $pos = strpos($db, '?')) {
                $db = substr($db, 0, $pos);
            }
            $db = trim($db, '/');
            if (empty($db)) {
                throw new BadRequestException("Database name must be provided or included in the dsn or as an 'option' attribute.");
            }
        }


        return parent::validate($data, $throwException);
    }

    /**
     * {@inheritdoc}
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'host':
                $schema['label'] = 'Host';
                $schema['description'] = 'The name of the database host, i.e. localhost, 192.168.1.1, etc.';
                break;
            case 'port':
                $schema['label'] = 'Port Number';
                $schema['description'] = 'The number of the database host port, i.e. 27017';
                break;
            case 'database':
                $schema['label'] = 'Database';
                $schema['description'] =
                    'The name of the database to connect to on the given server. This can be a lookup key.';
                break;
            case 'username':
                $schema['label'] = 'Username';
                $schema['description'] = 'The name of the database user. This can be a lookup key.';
                break;
            case 'password':
                $schema['label'] = 'Password';
                $schema['type'] = 'password';
                $schema['description'] = 'The password for the database user. This can be a lookup key.';
                break;
            case 'dsn':
                $schema['label'] = 'Connection String';
                $schema['description'] =
                    'Overrides all other settings except options. The connection string for the service, '.
                    'i.e. mongodb://[username:password@]host1[:port1][,host2[:port2:],...][/database][?options]. ' .
                    'The username, password, and database values can be added in the connection string or in the options below. ' .
                    'For further information, see https://docs.mongodb.com/manual/reference/connection-string/#connection-string-options.';
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