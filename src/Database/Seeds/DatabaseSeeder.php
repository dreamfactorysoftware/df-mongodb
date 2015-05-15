<?php
/**
 * This file is part of the DreamFactory Services Platform(tm) SDK For PHP
 *
 * DreamFactory Services Platform(tm) <http://github.com/dreamfactorysoftware/dsp-core>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace DreamFactory\Rave\MongoDb\Database\Seeds;

use DreamFactory\Rave\Database\Seeds\BaseModelSeeder;

class DatabaseSeeder extends BaseModelSeeder
{
    protected $modelClass = 'DreamFactory\\Rave\\Models\\ServiceType';

    protected $records = [
        [
            'name'           => 'mongo_db',
            'class_name'     => 'DreamFactory\\Rave\\MongoDb\\Services\\MongoDb',
            'config_handler' => 'DreamFactory\\Rave\\MongoDb\\Models\\MongoDbConfig',
            'label'          => 'MongoDB',
            'description'    => 'Database service for MongoDB connections.',
            'group'          => 'Databases',
            'singleton'      => false,
        ]
    ];
}
