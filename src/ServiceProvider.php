<?php
namespace DreamFactory\Core\MongoDb;

use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\MongoDb\Models\MongoDbConfig;
use DreamFactory\Core\MongoDb\Services\MongoDb;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Register the MongoDB service provider first
        $this->app->register(\Jenssegers\Mongodb\MongodbServiceProvider::class);

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'mongodb',
                    'label'           => 'MongoDB',
                    'description'     => 'Database service supporting MongoDB connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => MongoDbConfig::class,
                    'factory'         => function ($config) {
                        return new MongoDb($config);
                    },
                ])
            );
        });
    }

    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}
