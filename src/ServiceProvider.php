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
        // Ensure MongoDB package is loaded
        if (!class_exists('Jenssegers\Mongodb\MongoDBServiceProvider')) {
            // Try to load it from vendor
            $providerPath = base_path('vendor/jenssegers/mongodb/src/MongoDBServiceProvider.php');
            if (file_exists($providerPath)) {
                require_once $providerPath;
            }
        }

        // Register the MongoDB service provider
        if (class_exists('Jenssegers\Mongodb\MongoDBServiceProvider')) {
            $this->app->register(\Jenssegers\Mongodb\MongoDBServiceProvider::class);
        }

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
