<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
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

use DreamFactory\Library\Utility\Enums\Verbs;
use Rave\Models\Service;

class MongoDbServiceTest extends TestCase
{
    const SERVICE_NAME = 'mongo';

    protected static $do_teardown = false;

    protected static $staged = false;

    public function stage()
    {
        parent::stage();

        if ( !Service::whereName( static::SERVICE_NAME )->exists() )
        {
            // adds the migration and calls the seeder to add service type
            Artisan::call( 'migrate', [ '--path' => 'workbench/DreamFactory/MongoDb/database/migrations' ] );
            Artisan::call( 'db:seed', [ '--class' => 'DreamFactory\\MongoDb\\Database\\Seeds\\MongoDbSeeder' ] );

            Service::create(
                [
                    'name'        => static::SERVICE_NAME,
                    'label'       => 'MongoDB Database',
                    'description' => 'MongoDB database for testing',
                    'is_active'   => 1,
                    'type'        => 'mongo_db',
                    'config'      => [ 'dsn' => env( 'MONGODB_DSN' ), 'username' => env( 'MONGODB_USER' ), 'password' => env( 'MONGODB_PASSWORD' ) ]
                ]
            );
        }
    }

    public function tearDown()
    {
        if ( static::$do_teardown )
        {
            Service::whereType( 'mongo_db' )->delete();
        }

        parent::tearDown();
    }

    protected function buildPath( $path = '' )
    {
        return $this->prefix . '/' . static::SERVICE_NAME . '/' . $path;
    }

    /************************************************
     * Testing GET
     ************************************************/

    public function testDefaultResources()
    {
        $rs = $this->call( Verbs::GET, $this->buildPath() );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'resource', $data );
        $this->assertCount( 4, $data['resource'] );
//        $this->assert( '_schema', $data['resource'] );
//        $this->assertCount( 3, $data['resource'] );
//        $this->assertArrayHasKey( '_proc', $data['resource'] );
//        $this->assertCount( 3, $data['resource'] );
//        $this->assertArrayHasKey( '_func', $data['resource'] );
//        $this->assertCount( 3, $data['resource'] );
    }

    public function testSchemaEmpty()
    {
        $rs = $this->call( Verbs::GET, $this->buildPath( '_schema' ) );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'resource', $data );
        $this->assertEmpty( $data['resource'] );
    }

    public function testCreateTable()
    {
        $payload = '{
	"field": [
		{
			"name": "id",
			"type": "id"
		},
		{
			"name": "name",
			"type": "string",
			"is_unique": true,
			"allow_null": false
		},
		{
			"name": "complete",
			"type": "boolean",
			"allow_null": true
		}
	]
}';

        Schema::dropIfExists( 'todo' );

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_schema/todo' ), $payload );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'name', $data );
        $this->assertSame( 'todo', $data['name'] );
    }

    public function testGetRecordsEmpty()
    {
        $rs = $this->call( Verbs::GET, $this->buildPath( '_table/todo' ) );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'record', $data );
        $this->assertEmpty( $data['record'] );
    }

    public function testCreateRecords()
    {
        $payload = '{
	"record": [
		{
			"name": "test1",
			"complete": false
		},
		{
			"name": "test2",
			"complete": true
		},
		{
			"name": "test3"
		}
	]
}';
        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo' ), $payload );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 3, $data['record'] );
//        $this->assertResponseStatus( 201 );
    }

    public function testGetRecordById()
    {
        $rs = $this->call( Verbs::GET, $this->buildPath( '_table/todo/1' ) );
        $data = json_decode( $rs->getContent(), true );
        $this->assertTrue( $data['id'] == 1 );
    }

    public function testGetRecordsByIds()
    {
        $rs = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3' ) );
        $data = json_decode( $rs->getContent(), true );
        $ids = implode( ",", array_column( $data['record'], 'id' ) );
        $this->assertTrue( $ids == "1,2,3" );
    }

    public function testGetRecordsOverTunnel()
    {
        $payload = '{"ids":[1,2]}';

        $rs = $this->call( Verbs::POST, $this->buildPath( '_table/todo' ), [ ], [ ], [ ], [ "HTTP_X_HTTP_METHOD" => Verbs::GET ], $payload );

        $data = json_decode( $rs->getContent(), true );
        $ids = implode( ",", array_column( $data['record'], 'id' ) );
        print_r( $ids );
        $this->assertTrue( $ids == "1,2" );
    }

    public function testResourceNotFound()
    {
        $this->call( Verbs::GET, $this->buildPath( '_table/todo/5' ) );
        $this->assertResponseStatus( 404 );
    }

    /************************************************
     * Testing POST
     ************************************************/

    public function testCreateRecord()
    {
        $payload = '{"record":[{"name":"test4","complete":false}]}';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo' ), $payload );
//        $this->assertResponseStatus( 201 );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 1, $data['record'] );
    }

    public function testCreateRecordsNoWrap()
    {
        $payload = '[
		{
			"name": "test5",
			"complete": false
		},
		{
			"name": "test6",
			"complete": true
		}
	]';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo' ), $payload );
        $this->assertTrue( $rs->getContent() == '{"record":[{"id":5},{"id":6}]}' );
    }

    public function testCreateRecordReturnFields()
    {
        $payload = '{"record":[{"name":"test7","complete":true}]}';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo?fields=name,complete' ), $payload );
//        $this->assertResponseStatus( 201 );
        $data = json_decode( $rs->getContent(), true );
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 1, $data['record'] );
        $this->assertArrayHasKey( 'name', $data['record'][0] );
        $this->assertArrayHasKey( 'complete', $data['record'][0] );
    }

    public function testCreateRecordsWithContinue()
    {
        $payload = '{
	"record": [
		{
			"name": "test8",
			"complete": false
		},
		{
			"name": "test5",
			"complete": true
		},
		{
			"name": "test9",
			"complete": null
		}
	]
}';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo?continue=true' ), $payload );
        print_r( $rs );
        $this->assertContains( '{"error":{"context":{"error":[1],"record":[{"id":8},"SQLSTATE[23000]: ', $rs->getContent() );
        $this->assertContains( "Duplicate entry 'test5'", $rs->getContent() );
        $this->assertResponseStatus( 400 );
    }

    public function testCreateRecordsWithRollback()
    {
        $payload = '{
	"record": [
		{
			"name": "testRollback",
			"complete": false
		},
		{
			"name": "test5",
			"complete": true
		},
		{
			"name": "testAfter"
		}
	]
}';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo?rollback=true' ), $payload );

        $this->assertContains(
            '{"error":{"context":{"error":[1],"record":[{"id":11},"SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry \'test5\'',
            $rs->getContent()
        );
        $this->assertResponseStatus( 500 );
    }

    public function testCreateRecordBadRequest()
    {
        $payload = '{"record":[{
                        "name":"test1",
                        "complete":true
                    }]}';

        $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo' ), $payload );
        $this->assertResponseStatus( 500 );
    }

    public function testCreateRecordFailNotNullField()
    {
        $payload = '{"record":[{
                        "name":null,
                        "complete":true
                    }]}';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo' ), $payload );

        $this->assertResponseStatus( 400 );
        $this->assertContains( '{"error":{"context":null,"message":"Field \'name\' can not be NULL.","code":400}}', $rs->getContent() );
    }

    public function testCreateRecordFailMissingRequiredField()
    {
        $payload = '{"record":[{
                        "complete":true
                    }]}';

        $rs = $this->callWithPayload( Verbs::POST, $this->buildPath( '_table/todo' ), $payload );

        $this->assertResponseStatus( 400 );
        $this->assertContains( '{"error":{"context":null,"message":"Required field \'name\' can not be NULL.","code":400}}', $rs->getContent() );
    }

//    /************************************************
//     * Testing PUT
//     ************************************************/
//
//    public function testPUTRecordById()
//    {
//        $this->testPATCHRecordById( Verbs::PUT );
//    }
//
//    public function testPUTRecordByIds()
//    {
//        $this->testPATCHRecordByIds( Verbs::PUT );
//    }
//
//    public function testPUTRecordBulk()
//    {
//        $this->testPATCHRecordBulk( Verbs::PUT );
//    }
//
//    /************************************************
//     * Testing PATCH
//     ************************************************/
//
//    public function testPATCHRecordBulkWithRollback( $verb = Verbs::PATCH )
//    {
//        DB::table( "services" )->insert(
//            array(
//                [ "name" => "db2", "label" => "Database 2", "description" => "Local Database 2", "is_active" => 1, "type" => "mongo_db" ],
//                [ "name" => "db3", "label" => "Database 3", "description" => "Local Database 3", "is_active" => 1, "type" => "mongo_db" ]
//            )
//        );
//
//        $payload = '[{
//                        "id":1,
//                        "description":"unit-test-d1",
//                        "label":"unit-test-l1"
//                    },
//                    {
//                        "id":2,
//                        "name":"db",
//                        "description":"unit-test-d2",
//                        "label":"unit-test-l2"
//                    },
//                    {
//                        "id":3,
//                        "description":"unit-test-d3",
//                        "label":"unit-test-l3"
//                    }]';
//
//        $rs = $this->callWithPayload( $verb, $this->buildPath( '_table/todo?rollback=true'), $payload );
//        $this->assertContains(
//            '{"error":{"context":null,"message":"Failed to update resource: SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry \'db\' ',
//            $rs->getContent()
//        );
//        $this->assertResponseStatus( 500 );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//
//        $this->assertEquals( "Local Database,Local Database 2,Local Database 3", $dColumn );
//        $this->assertEquals( "Database,Database 2,Database 3", $lColumn );
//    }
//
//    public function testPATCHRecordById( $verb = Verbs::PATCH )
//    {
//        $payload = '{
//                        "description":"unit-test-string"
//                    }';
//
//        $rs = $this->callWithPayload( $verb, $this->buildPath( '_table/todo/1'), $payload );
//        $this->assertContains( '{"id":1}', $rs->getContent() );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo/1') );
//        $resultArray = json_decode( $result->getContent(), true );
//
//        $this->assertEquals( "unit-test-string", $resultArray['description'] );
//    }
//
//    public function testPATCHRecordByIds( $verb = Verbs::PATCH )
//    {
//        DB::table( "services" )->insert(
//            array(
//                [ "name" => "db2", "label" => "Database 2", "description" => "Local Database 2", "is_active" => 1, "type" => "mongo_db" ],
//                [ "name" => "db3", "label" => "Database 3", "description" => "Local Database 3", "is_active" => 1, "type" => "mongo_db" ]
//            )
//        );
//
//        $payload = '[{
//                        "description":"unit-test-description",
//                        "label":"unit-test-label"
//                    }]';
//
//        $rs = $this->callWithPayload( $verb, $this->buildPath( '_table/todo?ids=1,2,3'), $payload );
//        $this->assertContains( '{"record":[{"id":1},{"id":2},{"id":3}]}', $rs->getContent() );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//
//        $this->assertEquals( "unit-test-description,unit-test-description,unit-test-description", $dColumn );
//        $this->assertEquals( "unit-test-label,unit-test-label,unit-test-label", $lColumn );
//    }
//
//    public function testPATCHRecordBulk( $verb = Verbs::PATCH )
//    {
//        DB::table( "services" )->insert(
//            array(
//                [ "name" => "db2", "label" => "Database 2", "description" => "Local Database 2", "is_active" => 1, "type" => "mongo_db" ],
//                [ "name" => "db3", "label" => "Database 3", "description" => "Local Database 3", "is_active" => 1, "type" => "mongo_db" ]
//            )
//        );
//
//        $payload = '[{
//                        "id":1,
//                        "description":"unit-test-d1",
//                        "label":"unit-test-l1"
//                    },
//                    {
//                        "id":2,
//                        "description":"unit-test-d2",
//                        "label":"unit-test-l2"
//                    },
//                    {
//                        "id":3,
//                        "description":"unit-test-d3",
//                        "label":"unit-test-l3"
//                    }]';
//
//        $rs = $this->callWithPayload( $verb, $this->buildPath( '_table/todo'), $payload );
//        $this->assertContains( '{"record":[{"id":1},{"id":2},{"id":3}]}', $rs->getContent() );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//
//        $this->assertEquals( "unit-test-d1,unit-test-d2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,unit-test-l2,unit-test-l3", $lColumn );
//    }
//
//    public function testPATCHRecordBulkWithFields( $verb = Verbs::PATCH )
//    {
//        DB::table( "services" )->insert(
//            array(
//                [ "name" => "db2", "label" => "Database 2", "description" => "Local Database 2", "is_active" => 1, "type" => "mongo_db" ],
//                [ "name" => "db3", "label" => "Database 3", "description" => "Local Database 3", "is_active" => 1, "type" => "mongo_db" ]
//            )
//        );
//
//        $payload = '[{
//                        "id":1,
//                        "description":"unit-test-d1",
//                        "label":"unit-test-l1"
//                    },
//                    {
//                        "id":2,
//                        "description":"unit-test-d2",
//                        "label":"unit-test-l2"
//                    },
//                    {
//                        "id":3,
//                        "description":"unit-test-d3",
//                        "label":"unit-test-l3"
//                    }]';
//
//        $rs = $this->callWithPayload( $verb, $this->buildPath( '_table/todo?fields=label'), $payload );
//        $this->assertContains( '{"record":[{"label":"unit-test-l1"},{"label":"unit-test-l2"},{"label":"unit-test-l3"}]}', $rs->getContent() );
//    }
//
//    public function testPATCHRecordBulkWithContinue( $verb = Verbs::PATCH )
//    {
//        DB::table( "services" )->insert(
//            array(
//                [ "name" => "db2", "label" => "Database 2", "description" => "Local Database 2", "is_active" => 1, "type" => "mongo_db" ],
//                [ "name" => "db3", "label" => "Database 3", "description" => "Local Database 3", "is_active" => 1, "type" => "mongo_db" ]
//            )
//        );
//
//        $payload = '[{
//                        "id":1,
//                        "description":"unit-test-d1",
//                        "label":"unit-test-l1"
//                    },
//                    {
//                        "id":2,
//                        "name":"db",
//                        "description":"unit-test-d2",
//                        "label":"unit-test-l2"
//                    },
//                    {
//                        "id":3,
//                        "description":"unit-test-d3",
//                        "label":"unit-test-l3"
//                    }]';
//
//        $rs = $this->callWithPayload( $verb, $this->buildPath( '_table/todo?continue=1'), $payload );
//        $this->assertContains( '{"error":{"context":{"errors":[1],"record":[{"id":1},', $rs->getContent() );
//        $this->assertContains( ',{"id":3}]}', $rs->getContent() );
//        $this->assertResponseStatus( 400 );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//
//        $this->assertEquals( "unit-test-d1,Local Database 2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,Database 2,unit-test-l3", $lColumn );
//    }

    /************************************************
     * Testing DELETE
     ************************************************/

    public function testDeleteRecordById()
    {
        $rs = $this->call( Verbs::DELETE, $this->buildPath( '_table/todo/1' ) );
        $this->assertResponseOk();
//        $this->assertEquals( '{"id":1}', $rs->getContent() );

        $this->call( Verbs::GET, $this->buildPath( '_table/todo/1' ) );
        $this->assertResponseStatus( 404 );
    }

    public function testDeleteRecordByIds()
    {
        $rs = $this->call( Verbs::DELETE, $this->buildPath( '_table/todo?ids=2,3' ) );
        $this->assertResponseOk();
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $this->call( Verbs::GET, $this->buildPath( '_table/todo/2' ) );
        $this->assertResponseStatus( 404 );

        $rs = $this->call( Verbs::GET, $this->buildPath( '_table/todo/3' ) );
        $this->assertResponseStatus( 404 );
    }

//    public function testDELETERecordBulk()
//    {
//        $this->testCreateRecords();
//
//        $payload = '[{"id":2},{"id":3}]';
//
//        $rs = $this->callWithPayload( Verbs::DELETE, $this->buildPath( '_table/todo'), $payload );
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );
//
//        $rs = $this->call( Verbs::GET, $this->buildPath( '_table/todo/1') );
//        $data = json_decode( $rs->getContent(), true );
//        $this->assertEquals( "Database", $data['label'] );
//
//        $this->call( Verbs::GET, $this->buildPath( '_table/todo/3') );
//        $this->assertResponseStatus( 404 );
//    }
//
//    public function testDELETERecordBulkWithFields()
//    {
//        DB::table( "services" )->insert(
//            array(
//                [ "name" => "db2", "label" => "Database 2", "description" => "Local Database 2", "is_active" => 1, "type" => "mongo_db" ],
//                [ "name" => "db3", "label" => "Database 3", "description" => "Local Database 3", "is_active" => 1, "type" => "mongo_db" ]
//            )
//        );
//
//        $payload = '[{"id":2},{"id":3}]';
//
//        $rs = $this->callWithPayload( Verbs::DELETE, $this->buildPath( '_table/todo?fields=name,type'), $payload );
//        $this->assertEquals( '{"record":[{"name":"db2","type":"mongo_db"},{"name":"db3","type":"mongo_db"}]}', $rs->getContent() );
//    }
//
    public function testDropTable()
    {
        $rs = $this->call( Verbs::DELETE, $this->buildPath( '_schema/todo' ) );
        $this->assertResponseOk();

        $this->call( Verbs::GET, $this->buildPath( '_schema/todo' ) );
        $this->assertResponseStatus( 404 );
    }
}
