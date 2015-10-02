<?php
namespace DreamFactory\Core\MongoDb\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Database\TableNameSchema;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\MongoDb\Resources\Schema;
use DreamFactory\Core\MongoDb\Resources\Table;

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
    use DbSchemaExtras;

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
    protected $tableNames = [];
    /**
     * @var array
     */
    protected $tables = [];
    /**
     * @var array
     */
    protected $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
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
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        static::checkExtensions(['mongo']);

        $config = ArrayUtils::clean(ArrayUtils::get($settings, 'config'));
        Session::replaceLookups($config, true);

        $dsn = strval(ArrayUtils::get($config, 'dsn'));
        if (!empty($dsn)) {
            if (0 != substr_compare($dsn, static::DSN_PREFIX, 0, static::DSN_PREFIX_LENGTH, true)) {
                $dsn = static::DSN_PREFIX . $dsn;
            }
        }

        $options = ArrayUtils::get($config, 'options', []);
        if (empty($options)) {
            $options = [];
        }
        $user = ArrayUtils::get($config, 'username');
        $password = ArrayUtils::get($config, 'password');

        // support old configuration options of user, pwd, and db in credentials directly
        if (!isset($options['username']) && isset($user)) {
            $options['username'] = $user;
        }
        if (!isset($options['password']) && isset($password)) {
            $options['password'] = $password;
        }
        if (!isset($options['db']) && (null !== $db = ArrayUtils::get($config, 'db', null, true))) {
            $options['db'] = $db;
        }

        if (!isset($db) && (null === $db = ArrayUtils::get($options, 'db', null, true))) {
            //  Attempt to find db in connection string
            $db = strstr(substr($dsn, static::DSN_PREFIX_LENGTH), '/');
            if (false !== $pos = strpos($db, '?')) {
                $db = substr($db, 0, $pos);
            }
            $db = trim($db, '/');
        }

        if (empty($db)) {
            throw new InternalServerErrorException("No MongoDb database selected in configuration.");
        }

        $driverOptions = ArrayUtils::clean(ArrayUtils::get($config, 'driver_options'));
        if (null !== $context = ArrayUtils::get($driverOptions, 'context')) {
            //  Automatically creates a stream from context
            $driverOptions['context'] = stream_context_create($context);
        }

        try {
            $client = @new \MongoClient($dsn, $options, $driverOptions);

            $this->dbConn = $client->selectDB($db);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Unexpected MongoDb Service Exception:\n{$ex->getMessage()}");
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        try {
            $this->dbConn = null;
        } catch (\Exception $ex) {
            error_log("Failed to disconnect from database.\n{$ex->getMessage()}");
        }
    }

    /**
     * @throws \Exception
     */
    public function getConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->dbConn;
    }

    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        if ($refresh ||
            (empty($this->tableNames) &&
                (null === $this->tableNames = $this->getFromCache('table_names')))
        ) {
            /** @type TableNameSchema[] $names */
            $names = [];
            $tables = $this->dbConn->getCollectionNames();
            foreach ($tables as $table) {
                $names[strtolower($table)] = new TableNameSchema($table);
            }
            // merge db extras
            if (!empty($extrasEntries = $this->getSchemaExtrasForTables($tables, false))) {
                foreach ($extrasEntries as $extras) {
                    if (!empty($extraName = strtolower(strval($extras['table'])))) {
                        if (array_key_exists($extraName, $tables)) {
                            $names[$extraName]->fill($extras);
                        }
                    }
                }
            }
            $this->tableNames = $names;
            $this->addToCache('table_names', $this->tableNames, true);
        }

        return $this->tableNames;
    }

    public function refreshTableCache()
    {
        $this->removeFromCache('table_names');
        $this->tableNames = [];
        $this->tables = [];
    }
}