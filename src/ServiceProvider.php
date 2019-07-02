<?php
namespace DreamFactory\Core\MongoDb;

use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\MongoDb\Models\MongoDbConfig;
use DreamFactory\Core\MongoDb\Models\GridFsConfig;
use DreamFactory\Core\MongoDb\Services\GridFsService;
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
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'                  => 'mongodb',
                    'label'                 => 'MongoDB',
                    'description'           => 'Database service for MongoDB connections.',
                    'group'                 => ServiceTypeGroups::DATABASE,
                    'subscription_required' => LicenseLevel::SILVER,
                    'config_handler'        => MongoDbConfig::class,
                    'factory'               => function ($config) {
                        return new MongoDb($config);
                    },
                ])
            );
            $df->addType(
                new ServiceType([
                    'name'                  => 'gridfs',
                    'label'                 => 'GridFS',
                    'description'           => 'GridFS File Storage services.',
                    'group'                 => ServiceTypeGroups::FILE,
                    'subscription_required' => LicenseLevel::SILVER,
                    'config_handler'        => GridFsConfig::class,
                    'factory'               => function ($config) {
                        return new GridFsService($config);
                    },
                ])
            );
        });
    }

    public function boot()
    {
        parent::boot();

        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}
