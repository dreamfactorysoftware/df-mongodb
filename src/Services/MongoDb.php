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

namespace DreamFactory\Rave\MongoDb\Services;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Rave\Components\RequireExtensions;
use DreamFactory\Rave\Exceptions\BadRequestException;
use DreamFactory\Rave\Exceptions\InternalServerErrorException;
use DreamFactory\Rave\Exceptions\NotFoundException;
use DreamFactory\Rave\Contracts\ServiceResponseInterface;
use DreamFactory\Rave\Services\BaseNoSqlDbService;
use DreamFactory\Rave\Resources\BaseRestResource;
use DreamFactory\Rave\MongoDb\Resources\Schema;
use DreamFactory\Rave\MongoDb\Resources\Table;

/**
 * MongoDb
 *
 * A service to handle MongoDB NoSQL (schema-less) database
 * services accessed through the REST API.
 */
class MongoDb extends BaseNoSqlDbService
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use RequireExtensions;

    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Connection string prefix
     */
    const DSN_PREFIX = 'mongodb://';
    /**
     * Connection string prefix length
     */
    const DSN_PREFIX_LENGTH = 10;

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var \MongoDB
     */
    protected $dbConn = null;

    /**
     * @var array
     */
    protected $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Rave\\MongoDb\\Resources\\Schema',
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Rave\\MongoDb\\Resources\\Table',
            'label'      => 'Table',
        ],
    ];

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * Create a new MongoDbSvc
     *
     * @param array $settings
     *
     * @throws \InvalidArgumentException
     * @throws \Exception
     */
    public function __construct( $settings = [ ] )
    {
        parent::__construct( $settings );

        static::checkExtensions( [ 'mongo' ] );

        $config = ArrayUtils::clean( ArrayUtils::get( $settings, 'config' ) );
//        Session::replaceLookups( $config, true );

        $dsn = strval( ArrayUtils::get( $config, 'dsn' ) );
        if ( !empty( $dsn ) )
        {
            if ( 0 != substr_compare( $dsn, static::DSN_PREFIX, 0, static::DSN_PREFIX_LENGTH, true ) )
            {
                $dsn = static::DSN_PREFIX . $dsn;
            }
        }

        $options = ArrayUtils::get( $config, 'options', [ ] );
        if ( empty( $options ) )
        {
            $options = [ ];
        }
        $user = ArrayUtils::get( $config, 'username' );
        $password = ArrayUtils::get( $config, 'password' );

        // support old configuration options of user, pwd, and db in credentials directly
        if ( !isset( $options['username'] ) && isset( $user ) )
        {
            $options['username'] = $user;
        }
        if ( !isset( $options['password'] ) && isset( $password ) )
        {
            $options['password'] = $password;
        }
        if ( !isset( $options['db'] ) && ( null !== $db = ArrayUtils::get( $config, 'db', null, true ) ) )
        {
            $options['db'] = $db;
        }

        if ( !isset( $db ) && ( null === $db = ArrayUtils::get( $options, 'db', null, true ) ) )
        {
            //  Attempt to find db in connection string
            $db = strstr( substr( $dsn, static::DSN_PREFIX_LENGTH ), '/' );
            if ( false !== $_pos = strpos( $db, '?' ) )
            {
                $db = substr( $db, 0, $_pos );
            }
            $db = trim( $db, '/' );
        }

        if ( empty( $db ) )
        {
            throw new InternalServerErrorException( "No MongoDb database selected in configuration." );
        }

        $driverOptions = ArrayUtils::clean( ArrayUtils::get( $config, 'driver_options' ) );
        if ( null !== $context = ArrayUtils::get( $driverOptions, 'context' ) )
        {
            //  Automatically creates a stream from context
            $driverOptions['context'] = stream_context_create( $context );
        }

        try
        {
            $client = @new \MongoClient( $dsn, $options, $driverOptions );

            $this->dbConn = $client->selectDB( $db );
        }
        catch ( \Exception $_ex )
        {
            throw new InternalServerErrorException( "Unexpected MongoDb Service Exception:\n{$_ex->getMessage()}" );
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        try
        {
            $this->dbConn = null;
        }
        catch ( \Exception $_ex )
        {
            error_log( "Failed to disconnect from database.\n{$_ex->getMessage()}" );
        }
    }

    /**
     * @throws \Exception
     */
    public function getConnection()
    {
        if ( !isset( $this->dbConn ) )
        {
            throw new InternalServerErrorException( 'Database connection has not been initialized.' );
        }

        return $this->dbConn;
    }

    /**
     * @param string $name
     *
     * @return string
     * @throws BadRequestException
     * @throws NotFoundException
     */
    public function correctTableName( &$name )
    {
        static $_existing = null;

        if ( !$_existing )
        {
            $_existing = $this->dbConn->getCollectionNames();
        }

        if ( empty( $name ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        if ( false === array_search( $name, $_existing ) )
        {
            throw new NotFoundException( "Table '$name' not found." );
        }

        return $name;
    }

    /**
     * {@InheritDoc}
     */
    protected function handleResource( array $resources )
    {
        try
        {
            return parent::handleResource( $resources );
        }
        catch ( NotFoundException $_ex )
        {
            // If version 1.x, the resource could be a table
//            if ($this->request->getApiVersion())
//            {
//                $resource = $this->instantiateResource( 'DreamFactory\\Rave\\MongoDb\\Resources\\Table', [ 'name' => $this->resource ] );
//                $newPath = $this->resourceArray;
//                array_shift( $newPath );
//                $newPath = implode( '/', $newPath );
//
//                return $resource->handleRequest( $this->request, $newPath, $this->outputFormat );
//            }

            throw $_ex;
        }
    }

    /**
     * @return array
     */
    protected function getResources()
    {
        return $this->resources;
    }

    // REST service implementation

    /**
     * {@inheritdoc}
     */
    public function listResources( $fields = null )
    {
        if ( !$this->request->getParameterAsBool( 'as_access_components' ) )
        {
            return parent::listResources( $fields );
        }

        $_resources = [ ];

//        $refresh = $this->request->queryBool( 'refresh' );

        $_name = Schema::RESOURCE_NAME . '/';
        $_access = $this->getPermissions( $_name );
        if ( !empty( $_access ) )
        {
            $_resources[] = $_name;
            $_resources[] = $_name . '*';
        }

        $_result = $this->dbConn->getCollectionNames();
        foreach ( $_result as $_name )
        {
            $_name = Schema::RESOURCE_NAME . '/' . $_name;
            $_access = $this->getPermissions( $_name );
            if ( !empty( $_access ) )
            {
                $_resources[] = $_name;
            }
        }

        $_name = Table::RESOURCE_NAME . '/';
        $_access = $this->getPermissions( $_name );
        if ( !empty( $_access ) )
        {
            $_resources[] = $_name;
            $_resources[] = $_name . '*';
        }

        foreach ( $_result as $_name )
        {
            $_name = Table::RESOURCE_NAME . '/' . $_name;
            $_access = $this->getPermissions( $_name );
            if ( !empty( $_access ) )
            {
                $_resources[] = $_name;
            }
        }

        return [ 'resource' => $_resources ];
    }

    /**
     * @return ServiceResponseInterface
     */
//    protected function respond()
//    {
//        if ( Verbs::POST === $this->getRequestedAction() )
//        {
//            switch ( $this->resource )
//            {
//                case Table::RESOURCE_NAME:
//                case Schema::RESOURCE_NAME:
//                    if ( !( $this->response instanceof ServiceResponseInterface ) )
//                    {
//                        $this->response = ResponseFactory::create( $this->response, $this->outputFormat, ServiceResponseInterface::HTTP_CREATED );
//                    }
//                    break;
//            }
//        }
//
//        parent::respond();
//    }

}