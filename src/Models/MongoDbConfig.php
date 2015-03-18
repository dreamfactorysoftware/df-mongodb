<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DreamFactory\MongoDb\Models;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Rave\Exceptions\BadRequestException;
use DreamFactory\Rave\Contracts\ServiceConfigHandlerInterface;
use DreamFactory\Rave\Models\BaseModel;
use Illuminate\Database\Query\Builder;

/**
 * MongoDbConfig
 *
 * @property integer $service_id
 * @property string  $dsn
 * @property string  $options
 * @property string  $driver_options
 *
 * @method static Builder|MongoDbConfig whereServiceId( $value )
 */
class MongoDbConfig extends BaseModel implements ServiceConfigHandlerInterface
{
    protected $table = 'mongo_db_config';

    protected $primaryKey = 'service_id';

    protected $fillable = [ 'service_id', 'dsn', 'options', 'driver_options' ];

    public $timestamps = false;

    public $incrementing = false;

    public static function getConfig( $id )
    {
        $model = static::find( $id );

        return ( !empty( $model ) ) ? $model->toArray() : [ ];
    }

    public static function validateConfig( $config )
    {
        if ( ( null === ArrayUtils::get( $config, 'dsn', null, true ) ) )
        {
            if ( ( null === ArrayUtils::getDeep( $config, 'options', 'db', null, true ) ) )
            {
                throw new BadRequestException( 'Database name must be included in the \'dsn\' or as an \'option\' attribute.' );
            }
        }

        return true;
    }

    public static function setConfig( $id, $config )
    {
        $model = static::find( $id );
        if ( !empty( $model ) )
        {
            $model->update( $config );
        }
        else
        {
            $config['service_id'] = $id;
            static::create( $config );
        }
    }

    public static function removeConfig( $id )
    {
        // deleting is not necessary here due to cascading on_delete relationship in database
    }

    public static function getAvailableConfigs()
    {
        return null;
    }

}