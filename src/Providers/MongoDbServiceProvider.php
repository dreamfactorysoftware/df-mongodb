<?php
namespace DreamFactory\MongoDb\Providers;

use DreamFactory\MongoDb\Database\Connection;

class MongoDbServiceProvider extends \Jenssegers\Mongodb\MongodbServiceProvider
{
    public function register()
    {
        parent::register();

        // Add our database drivers.
        $this->app->resolving('db', function ($db){
            $db->extend('mongodb', function ($config){
                Connection::adaptConfig($config);

                return new Connection($config);
            });
        });
    }
}
