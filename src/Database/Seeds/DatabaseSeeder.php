<?php
namespace DreamFactory\Core\MongoDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;
use DreamFactory\Core\Models\ServiceType;
use DreamFactory\Core\MongoDb\Models\MongoDbConfig;
use DreamFactory\Core\MongoDb\Services\MongoDb;

class DatabaseSeeder extends BaseModelSeeder
{
    protected $modelClass = ServiceType::class;

    protected $records = [
        [
            'name'           => 'mongo_db',
            'class_name'     => MongoDb::class,
            'config_handler' => MongoDbConfig::class,
            'label'          => 'MongoDB',
            'description'    => 'Database service for MongoDB connections.',
            'group'          => 'Databases',
            'singleton'      => false,
        ]
    ];
}
