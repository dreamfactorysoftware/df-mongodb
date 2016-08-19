<?php
namespace DreamFactory\Core\MongoDb;

use DreamFactory\Core\Components\ServiceDocBuilder;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\MongoDb\Models\MongoDbConfig;
use DreamFactory\Core\MongoDb\Services\MongoDb;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use Jenssegers\Mongodb\MongodbServiceProvider;

class ServiceProvider extends MongodbServiceProvider
{
    use ServiceDocBuilder;

    public function register()
    {
        parent::register();

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'mongodb',
                    'label'           => 'MongoDB',
                    'description'     => 'Database service for MongoDB connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => MongoDbConfig::class,
                    'default_api_doc' => function ($service) {
                        return $this->buildServiceDoc($service->id, MongoDb::getApiDocInfo($service));
                    },
                    'factory'         => function ($config) {
                        return new MongoDb($config);
                    },
                ])
            );
        });
    }
}
