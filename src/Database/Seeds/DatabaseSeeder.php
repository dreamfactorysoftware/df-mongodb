<?php
namespace DreamFactory\Core\MongoDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;

class DatabaseSeeder extends BaseModelSeeder
{
    protected $modelClass = 'DreamFactory\\Core\\Models\\ServiceType';

    protected $records = [
        [
            'name'           => 'mongo_db',
            'class_name'     => 'DreamFactory\\Core\\MongoDb\\Services\\MongoDb',
            'config_handler' => 'DreamFactory\\Core\\MongoDb\\Models\\MongoDbConfig',
            'label'          => 'MongoDB',
            'description'    => 'Database service for MongoDB connections.',
            'group'          => 'Databases',
            'singleton'      => false,
        ]
    ];
}
