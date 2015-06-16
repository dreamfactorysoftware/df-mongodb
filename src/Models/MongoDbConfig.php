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

    protected $table = 'mongo_db_config';

    protected $fillable = ['service_id', 'dsn', 'options', 'driver_options'];

    protected $casts = ['options' => 'array', 'driver_options' => 'array'];

    public static function validateConfig($config)
    {
        static::checkExtensions(['mongo']);

        if ((null === ArrayUtils::get($config, 'dsn', null, true))) {
            if ((null === ArrayUtils::getDeep($config, 'options', 'db', null, true))) {
                throw new BadRequestException('Database name must be included in the \'dsn\' or as an \'option\' attribute.');
            }
        }

        return true;
    }
}