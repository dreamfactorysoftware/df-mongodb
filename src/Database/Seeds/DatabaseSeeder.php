<?php
namespace DreamFactory\Rave\MongoDb\Database\Seeds;

use Illuminate\Database\Seeder;
use DreamFactory\Rave\Models\ServiceType;

class DatabaseSeeder extends Seeder
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
                'class_name'     => 'DreamFactory\\Rave\\MongoDb\\Services\\MongoDbService',
                'config_handler' => 'DreamFactory\\Rave\\MongoDb\\Models\\MongoDbConfig',
                'label'          => 'MongoDB',
                'description'    => 'Database service for MongoDB connections.',
                'group'          => 'Databases',
                'singleton'      => false,
            ]
        );
        $this->command->info( 'MongoDb service type seeded!' );
    }

}
