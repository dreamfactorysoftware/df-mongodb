<?php
namespace DreamFactory\MongoDb\Database\Seeds;

use Illuminate\Database\Seeder;
use DreamFactory\Rave\Models\ServiceType;

class MongoDbSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        // Add the service type
        ServiceType::create(
            [
                'name'           => 'mongo_db',
                'class_name'     => 'DreamFactory\\MongoDb\\Services\\MongoDbService',
                'config_handler' => 'DreamFactory\\MongoDb\\Models\\MongoDbConfig',
                'label'          => 'MongoDB',
                'description'    => 'Database service for MongoDB connections.',
                'group'          => 'Databases',
                'singleton'      => false,
            ]
        );
        $this->command->info( 'MongoDb service type seeded!' );
    }

}
