<?php
namespace DreamFactory\Core\MongoDb;

use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\MongoDb\Models\MongoDbConfig;
use DreamFactory\Core\MongoDb\Services\MongoDb;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use Jenssegers\Mongodb\MongodbServiceProvider;

class ServiceProvider extends MongodbServiceProvider
{
    public function register()
    {
        parent::register();

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df){
            $df->addType(
                new ServiceType([
                    'name'           => 'mongodb',
                    'label'          => 'MongoDB',
                    'description'    => 'Database service for MongoDB connections.',
                    'group'          => ServiceTypeGroups::DATABASE,
                    'config_handler' => MongoDbConfig::class,
                    'factory'        => function ($config){
                        return new MongoDb($config);
                    },
                ])
            );
        });
    }
}
