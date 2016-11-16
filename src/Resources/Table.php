<?php
namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\RelationSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\MongoDb\Services\MongoDb;
use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Library\Utility\Scalar;
use MongoDB\BSON\Binary;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\Regex;
use MongoDB\BSON\Timestamp;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Model\BSONDocument;
use MongoDB\Operation\FindOneAndReplace;
use MongoDB\Operation\FindOneAndUpdate;

class Table extends BaseNoSqlDbTableResource
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Default record identifier field
     */
    const DEFAULT_ID_FIELD = '_id';

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|MongoDb
     */
    protected $parent = null;
    /**
     * @var Collection
     */
    protected $collection = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    protected function getOptionalParameters()
    {
        return array_merge(parent::getOptionalParameters(), []);
    }

    /**
     * @return null|MongoDb
     */
    public function getService()
    {
        return $this->parent;
    }

    /**
     * @param $name
     *
     * @return Collection|null
     */
    public function selectTable($name)
    {
        /** @noinspection PhpUndefinedMethodInspection */
        $coll = $this->parent->getConnection()->getCollection($name);

        return $coll;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');
        $coll = $this->selectTable($table);

        $ssFilters = array_get($extras, 'ss_filters');

        static::removeIds($record, static::DEFAULT_ID_FIELD);
        $fieldsInfo = $this->getFieldsInfo($table);
        $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);
        if (empty($parsed)) {
            throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
        }

        // build criteria from filter parameters
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            $result = $coll->updateMany($criteria, $parsed);
            if ($result->getMatchedCount() > 0) {
                return $this->runQuery($table, $criteria, $extras);
            }

            return [];
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');
        $coll = $this->selectTable($table);

        $ssFilters = array_get($extras, 'ss_filters');

        static::removeIds($record, static::DEFAULT_ID_FIELD);
        $fieldsInfo = $this->getFieldsInfo($table);
        if (!static::doesRecordContainModifier($record)) {
            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);
            if (empty($parsed)) {
                throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
            }

            $parsed = ['$set' => $parsed];
        } else {
            $parsed = $record;
            if (empty($parsed)) {
                throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
            }
        }

        // build criteria from filter parameters
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            $result = $coll->updateMany($criteria, $parsed);
            if ($result->getMatchedCount() > 0) {
                return $this->runQuery($table, $criteria, $extras);
            }

            return [];
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($table, $extras = [])
    {
        $coll = $this->selectTable($table);
        try {
            // build filter string if necessary, add server-side filters if necessary
            $ssFilters = array_get($extras, 'ss_filters');
            $criteria = $this->buildCriteriaArray([], null, $ssFilters);
            $coll->deleteMany($criteria);

            return ['success' => true];
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRecordsByFilter($table, $filter, $params = [], $extras = [])
    {
        if (empty($filter)) {
            throw new BadRequestException("Filter for delete request can not be empty.");
        }

        $coll = $this->selectTable($table);

        $ssFilters = array_get($extras, 'ss_filters');

        // build criteria from filter parameters
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            $data = $this->runQuery($table, $criteria, $extras);
            $coll->deleteMany($criteria);

            return $data;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $schema = $this->parent->getSchema()->getResource(DbResourceTypes::TYPE_TABLE, $table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $ssFilters = array_get($extras, 'ss_filters');
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            return $this->runQuery($table, $criteria, $extras);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to filter records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        $requested_fields = static::DEFAULT_ID_FIELD; // can only be this
        $requested_types =
            (empty($requested_types) ? [] : (!is_array($requested_types) ? [$requested_types] : $requested_types));
        $type = array_get($requested_types, 0, 'string');
        $type = (empty($type)) ? 'string' : $type;

        return [new ColumnSchema(['name' => static::DEFAULT_ID_FIELD, 'type' => $type, 'required' => false])];
    }

    /**
     * @param $record
     *
     * @return bool
     */
    protected static function doesRecordContainModifier($record)
    {
        if (is_array($record)) {
            foreach ($record as $key => $value) {
                if (!empty($key) && ('$' == $key[0])) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param string|array $include List of keys to include in the output record
     *
     * @return null|array
     */
    protected static function buildProjection($include = '*')
    {
        if ('*' == $include) {
            return null;
        }

        if (empty($include)) {
            $include = static::DEFAULT_ID_FIELD;
        }
        if (!is_array($include)) {
            $include = array_map('trim', explode(',', trim($include, ',')));
        }
        if (false === array_search(static::DEFAULT_ID_FIELD, $include)) {
            $include[] = static::DEFAULT_ID_FIELD;
        }

        $out = [];
        foreach ($include as $key) {
            $out[$key] = 1;
        }

        return $out;
    }

    public static function localizeOperator($operator)
    {
        switch ($operator) {
            // Logical
            case DbLogicalOperators::AND_SYM:
            case DbLogicalOperators::AND_STR:
                return '$and';
            case DbLogicalOperators::OR_SYM:
            case DbLogicalOperators::OR_STR:
                return '$or';
            case DbLogicalOperators::NOR_STR:
                return '$nor';
            case DbLogicalOperators::NOT_STR:
                return '$not';
            // Comparison
            case DbComparisonOperators::EQ_STR:
            case DbComparisonOperators::EQ:
                return '$eq';
            case DbComparisonOperators::NE_STR:
            case DbComparisonOperators::NE:
            case DbComparisonOperators::NE_2:
                return '$ne';
            case DbComparisonOperators::GT_STR:
            case DbComparisonOperators::GT:
                return '$gt';
            case DbComparisonOperators::GTE_STR:
            case DbComparisonOperators::GTE:
                return '$gte';
            case DbComparisonOperators::LT_STR:
            case DbComparisonOperators::LT:
                return '$lt';
            case DbComparisonOperators::LTE_STR:
            case DbComparisonOperators::LTE:
                return '$lte';
            case DbComparisonOperators::IN:
                return '$in';
            case DbComparisonOperators::NOT_IN:
                return '$nin';
            default:
                return $operator;
        }
    }

    /**
     * @param string|array $filter Filter for querying records by
     * @param array        $params Filter replacement parameters
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected static function buildFilterArray($filter, $params = null)
    {
        if (empty($filter)) {
            return [];
        }

        if (is_array($filter)) {
            // assume client knows correct usage of Mongo query language
            return static::toMongoObjects($filter);
        }

        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1)  or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . '(')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = static::buildFilterArray($parts, $params);

                    return [static::localizeOperator($logicalOp) => $parts];
                }
            } else {
                // (a = 1) AND (b = 2) format
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos));
                    $right = trim(substr($filter, $pos + strlen($paddedOp)));
                    $left = static::buildFilterArray($left, $params);
                    $right = static::buildFilterArray($right, $params);

                    return [static::localizeOperator($logicalOp) => [$left, $right]];
                }
                // (a = 1)AND(b = 2) format
                $paddedOp = ')' . $logicalOp . '(';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos));
                    $right = trim(substr($filter, $pos + strlen($paddedOp)));
                    $left = static::buildFilterArray($left, $params);
                    $right = static::buildFilterArray($right, $params);

                    return [static::localizeOperator($logicalOp) => [$left, $right]];
                }
            }
        }

        $filter = trim(trim($filter, '()'));

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    if ((count($parts) > 2) || (0 !== strcasecmp($parts[1], trim(DbLogicalOperators::NOT_STR)))) {
                        // invalid field side of operator
                        throw new BadRequestException('Invalid or unparsable field in filter request.');
                    }
                    $field = $parts[0];
                    $negate = true;
                }

                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    $value = trim($value, '()[]');
                    $parsed = [];
                    foreach (explode(',', $value) as $each) {
                        $parsed[] = static::determineValue($each, $field, $params);
                    }
                    $value = $parsed;
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    switch ($sqlOp) {
                        case DbComparisonOperators::IS_NULL:
                            return [$field => null];
                        case DbComparisonOperators::IS_NOT_NULL:
                            return [$field => ['$ne' => null]];
                        case DbComparisonOperators::DOES_EXIST:
                            return [$field => ['$exists' => true]];
                        case DbComparisonOperators::DOES_NOT_EXIST:
                            return [$field => ['$exists' => false]];
                    }
                } else {
                    $value = static::determineValue($value, $field, $params);
                    if ('$eq' === static::localizeOperator($sqlOp)) {
                        // prior to 3.0
                        if ($negate) {
                            return [$field => ['$ne' => $value]];
                        }

                        return [$field => $value];
                    } elseif (DbComparisonOperators::LIKE === $sqlOp) {
//			WHERE name LIKE "%Joe%"	(array("name" => new Regex("/Joe/")));
//			WHERE name LIKE "Joe%"	(array("name" => new Regex("/^Joe/")));
//			WHERE name LIKE "%Joe"	(array("name" => new Regex("/Joe$/")));
                        if ('%' == $value[strlen($value) - 1]) {
                            if ('%' == $value[0]) {
                                $value = trim($value, '%');
                            } else {
                                $value = '^' . rtrim($value, '%');
                            }
                        } else {
                            if ('%' == $value[0]) {
                                $value = trim($value, '%') . '$';
                            }
                        }

                        return [$field => new Regex($value, '')];
                    } elseif (DbComparisonOperators::CONTAINS === $sqlOp) {
                        return [$field => new Regex($value, '')];
                    } elseif (DbComparisonOperators::STARTS_WITH === $sqlOp) {
                        return [$field => new Regex('^' . $value, '')];
                    } elseif (DbComparisonOperators::ENDS_WITH === $sqlOp) {
                        return [$field => new Regex($value . '$', '')];
                    }
                }

                if ($negate) {
                    $value = ['$not' => $value];
                }

                return [$field => [static::localizeOperator($sqlOp) => $value]];
            }
        }

        return $filter;
    }

    /**
     * @param string $value
     * @param string $field
     * @param array  $replacements
     *
     * @return bool|float|int|string|ObjectID
     */
    private static function determineValue($value, $field = null, $replacements = null)
    {
        // process parameter replacements
        if (is_string($value) && !empty($value) && (':' == $value[0])) {
            if (isset($replacements, $replacements[$value])) {
                $value = $replacements[$value];
            }
        }

        if ($field && (static::DEFAULT_ID_FIELD == $field)) {
            $value = static::idToMongoId($value);
        }

        if (is_string($value)) {
            if (trim($value, "'\"") !== $value) {
                return trim($value, "'\""); // meant to be a string
            }

            if (is_numeric($value)) {
                return ($value == strval(intval($value))) ? intval($value) : floatval($value);
            }

            if (0 == strcasecmp($value, 'true')) {
                return true;
            }

            if (0 == strcasecmp($value, 'false')) {
                return false;
            }
        }

        return $value;
    }

    /**
     * @param      $filter
     * @param null $params
     * @param null $ss_filters
     *
     * @return array|mixed
     * @throws InternalServerErrorException
     */
    protected static function buildCriteriaArray($filter, $params = null, $ss_filters = null)
    {
        // interpret any parameter values as lookups
        $params = static::interpretRecordValues($params);
        // or as Mongo objects
        $params = static::toMongoObjects($params);

        // build filter array if necessary
        if (!is_array($filter)) {
            Session::replaceLookups($filter);
            $test = json_decode($filter, true);
            if (!is_null($test)) {
                // original filter was a json string, use it as array
                $filter = $test;
            }
        }
        $criteria = static::buildFilterArray($filter, $params);

        // add server-side filters if necessary
        $serverCriteria = static::buildSSFilterArray($ss_filters);
        if (!empty($serverCriteria)) {
            $criteria = (!empty($criteria)) ? ['$and' => [$criteria, $serverCriteria]] : $serverCriteria;
        }

        return $criteria;
    }

    /**
     * @param $ss_filters
     *
     * @return array
     * @throws InternalServerErrorException
     */
    protected static function buildSSFilterArray($ss_filters)
    {
        if (empty($ss_filters)) {
            return null;
        }

        // build the server side criteria
        $filters = array_get($ss_filters, 'filters');
        if (empty($filters)) {
            return null;
        }

        $criteria = [];
        $combiner = array_get($ss_filters, 'filter_op', 'and');
        foreach ($filters as $filter) {
            $name = array_get($filter, 'name');
            $op = array_get($filter, 'operator');
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            $value = array_get($filter, 'value');
            $value = static::interpretFilterValue($value);

            $criteria[] = static::buildFilterArray("$name $op $value");
        }

        if (1 == count($criteria)) {
            return $criteria[0];
        }

        switch (strtoupper($combiner)) {
            case 'AND':
                $criteria = ['$and' => $criteria];
                break;
            case 'OR':
                $criteria = ['$or' => $criteria];
                break;
            case 'NOR':
                $criteria = ['$nor' => $criteria];
                break;
            default:
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
        }

        return $criteria;
    }

    /**
     * @param string|array $sort List of fields to sort the output records by
     *
     * @return array
     */
    protected static function buildSortArray($sort)
    {
        if (empty($sort)) {
            return null;
        }

        if (!is_array($sort)) {
            $sort = array_map('trim', explode(',', trim($sort, ',')));
        }
        $out = [];
        foreach ($sort as $combo) {
            if (!is_array($combo)) {
                $combo = array_map('trim', explode(' ', trim($combo, ' ')));
            }
            $dir = 1;
            $field = '';
            switch (count($combo)) {
                case 1:
                    $field = $combo[0];
                    break;
                case 2:
                    $field = $combo[0];
                    switch ($combo[1]) {
                        case -1:
                        case 'desc':
                        case 'DESC':
                        case 'dsc':
                        case 'DSC':
                            $dir = -1;
                            break;
                    }
            }
            if (!empty($field)) {
                $out[$field] = $dir;
            }
        }

        return $out;
    }

    /**
     * @param array|object|BSONDocument $record
     * @param string|array              $include List of keys to include in the output record
     * @param string|array              $id_field
     *
     * @return array
     */
    protected static function cleanRecord($record = [], $include = '*', $id_field = null)
    {
        if ($record instanceof BSONDocument) {
            $record = $record->getArrayCopy();
        } elseif (is_object($record)) {
            $record = (array)$record; // std object
        }
        $out = parent::cleanRecord($record, $include, $id_field);

        return static::fromMongoObjects($out);
    }

    /**
     * @param array|object|BSONDocument $record
     *
     * @return array|string
     */
    protected static function fromMongoObjects($record)
    {
        if ($record instanceof BSONDocument) {
            $record = $record->getArrayCopy();
        } elseif (is_object($record)) {
            $record = (array)$record; // std object
        }
        if (!empty($record)) {
            foreach ($record as &$data) {
                if (is_object($data)) {
                    if ($data instanceof ObjectID) {
                        $data = (string)$data;
                    } elseif ($data instanceof Timestamp) {
                        $data = (string)$data;
                    } elseif ($data instanceof UTCDateTime) {
                        if (empty($cfgFormat = DataFormatter::getDateTimeFormat('datetime'))) {
                            $cfgFormat = 'c';
                        }
                        $data = $data->toDateTime();
                        $data = ['$date' => $data->format($cfgFormat)];
                    } elseif ($data instanceof Binary) {
                        $data = $data->getData();
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @param array $record
     *
     * @return array
     */
    protected static function toMongoObjects($record)
    {
        if (!empty($record)) {
            foreach ($record as $key => $data) {
                if (!is_object($data)) {
                    if (is_string($data) && (static::DEFAULT_ID_FIELD == $key)) {
                        $record[$key] = static::idToMongoId($data);
                    } elseif (is_array($data)) {
                        if (1 === count($data)) {
                            // using typed definition, i.e. {"$date" : "2014-08-02T08:40:12.569Z" }
                            if (array_key_exists('$date', $data)) {
                                $temp = $data['$date'];
                                if (empty($temp)) {
                                    // empty means create with current time
                                    $record[$key] = new UTCDateTime(time() * 1000);
                                } elseif (is_string($temp)) {
                                    $record[$key] = new UTCDateTime(strtotime($temp) * 1000);
                                } elseif (is_int($temp)) {
                                    $record[$key] = new UTCDateTime($temp * 1000);
                                }
                            } elseif (isset($data['$id'])) {
                                $record[$key] = static::idToMongoId($data['$id']);
                            } else {
                                $record[$key] = static::toMongoObjects($data);
                            }
                        }
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @param mixed $value
     *
     * @return array|string
     */
    protected static function mongoIdToId($value)
    {
        if (is_object($value)) {
            /** $record ObjectID */
            $value = (string)$value;
        }

        return $value;
    }

    /**
     * @param array $records
     *
     * @return mixed
     */
    protected static function mongoIdsToIds($records)
    {
        foreach ($records as $key => $record) {
            $records[$key] = static::mongoIdToId($record);
        }

        return $records;
    }

    /**
     * @param mixed $value
     *
     * @return array|bool|float|int|ObjectID|string
     */
    protected static function idToMongoId($value)
    {
        if (is_array($value)) {
            if (array_key_exists('$id', $value)) {
                $value = array_get($value, '$id');
            }
        }

        if (is_string($value)) {
            $quoted = false;
            if (trim($value, "'\"") !== $value) {
                $value = trim($value, "'\""); // meant to be a string
                $quoted = true;
            }
            if ((24 == strlen($value))) {
                try {
                    $temp = new ObjectID($value);
                    $value = $temp;
                } catch (\Exception $ex) {
                    // obviously not a Mongo created Id, let it be
                    if ($quoted) {
                        $value = "'$value'";
                    }
                }
            } else {
                if ($quoted) {
                    $value = "'$value'";
                }
            }
        }

        return $value;
    }

    /**
     * @param string|array $ids
     *
     * @return array
     */
    protected static function idsToMongoIds($ids)
    {
        if (!is_array($ids)) {
            // comma delimited list of ids
            $ids = array_map('trim', explode(',', trim($ids, ',')));
        }

        foreach ($ids as &$id) {
            $id = static::idToMongoId($id);
        }

        return $ids;
    }

    protected function getCurrentTimestamp()
    {
        return new UTCDateTime(time() * 1000);
    }

    /**
     * @param array $record
     * @param array $fields_info
     * @param array $filter_info
     * @param bool  $for_update
     * @param array $old_record
     *
     * @return array
     * @throws \Exception
     */
    protected function parseRecord($record, $fields_info, $filter_info = null, $for_update = false, $old_record = null)
    {
        switch ($this->getAction()) {
            case Verbs::MERGE:
            case Verbs::PATCH:
                if (static::doesRecordContainModifier($record)) {
                    return $this->interpretRecordValues($record);
                }
                break;
        }

        // convert to native format
        $result = parent::parseRecord($record, $fields_info, $filter_info, $for_update, $old_record);

        return static::toMongoObjects($result);
    }

    /**
     * {@inheritdoc}
     */
    protected function initTransaction($table_name, &$id_fields = null, $id_types = null, $require_ids = true)
    {
        $this->collection = $this->selectTable($table_name);

        return parent::initTransaction($table_name, $id_fields, $id_types, $require_ids);
    }

    /**
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ) {
        $ssFilters = array_get($extras, 'ss_filters');
        $fields = array_get($extras, ApiOptions::FIELDS);
        $related = array_get($extras, 'related');
        $requireMore = Scalar::boolval(array_get($extras, 'require_more')) || !empty($related);
        $allowRelatedDelete = Scalar::boolval(array_get($extras, 'allow_related_delete'));
        $relatedInfo = $this->describeTableRelated($this->transactionTable);
        $updates = array_get($extras, 'updates');
        $options = [];

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo);
                }

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                if (!$continue && !$rollback && !$single) {
                    return parent::addToTransaction($parsed, $id);
                }

                $result = $this->collection->insertOne($parsed, $options);
                $id = $result->getInsertedId();
                if ($requireMore) {
                    $parsed[static::DEFAULT_ID_FIELD] = $id;
                    $out = static::cleanRecord($parsed, $fields, static::DEFAULT_ID_FIELD);
                } else {
                    $out = [static::DEFAULT_ID_FIELD => static::mongoIdToId($id)];
                }

                if (!empty($relatedInfo)) {
                    $this->updatePostRelations(
                        $this->transactionTable,
                        $record,
                        $relatedInfo,
                        $allowRelatedDelete
                    );
                }

                if ($rollback) {
                    $this->addToRollback($id);
                }
                break;

            case Verbs::PUT:
                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo);
                }
                if (!empty($updates)) {
                    $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                    $updates = $parsed;
                } else {
                    $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);
                }
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                // only update/patch by ids can use batching
                if (!$continue && !$rollback && !$single && !empty($updates)) {
                    return parent::addToTransaction(null, static::idToMongoId($id));
                }

                if (empty($updates)) {
                    static::removeIds($parsed, static::DEFAULT_ID_FIELD);
                    $updates = $parsed;
                }

                // simple overwrite existing record
                $filter = [static::DEFAULT_ID_FIELD => static::idToMongoId($id)];
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                if ($requireMore || $rollback) {
                    if (!$rollback) {
                        $options['projection'] = static::buildProjection($fields);
                        $options['returnDocument'] = FindOneAndReplace::RETURN_DOCUMENT_AFTER;
                    }
                    if (null === $result = $this->collection->findOneAndReplace($criteria, $updates, $options)) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    }

                    if ($rollback) {
                        $this->addToRollback($result);
                        // need to retrieve the full record here
                        if ($requireMore) {
                            $result = $this->collection->findOne($criteria, $options);
                            $out = static::cleanRecord($result, $fields, static::DEFAULT_ID_FIELD);
                        } else {
                            $out = [static::DEFAULT_ID_FIELD => $id];
                        }
                    } else {
                        $out = static::fromMongoObjects($result);
                    }

                    if (!empty($relatedInfo)) {
                        $this->updatePostRelations(
                            $this->transactionTable,
                            $out,
                            $relatedInfo,
                            $allowRelatedDelete
                        );
                    }
                } else {
                    $result = $this->collection->replaceOne($criteria, $updates, $options);
                    if (1 > $result->getMatchedCount()) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    }
                    $out = [static::DEFAULT_ID_FIELD => $id];
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo);
                }
                if (!empty($updates)) {
                    $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                    $updates = $parsed;
                } else {
                    $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);
                }
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                // only update/patch by ids can use batching
                if (!$continue && !$rollback && !$single && !empty($updates)) {
                    return parent::addToTransaction(null, static::idToMongoId($id));
                }

                if (empty($updates)) {
                    static::removeIds($parsed, static::DEFAULT_ID_FIELD);
                    $updates = $parsed;
                }

                $updates = ['$set' => $updates];

                // simple merge with existing record
                $filter = [static::DEFAULT_ID_FIELD => static::idToMongoId($id)];
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                if ($requireMore || $rollback) {
                    if (!$rollback) {
                        $options['projection'] = static::buildProjection($fields);
                        $options['returnDocument'] = FindOneAndUpdate::RETURN_DOCUMENT_AFTER;
                    }
                    if (null === $result = $this->collection->findOneAndUpdate($criteria, $updates, $options)) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    }

                    if ($rollback) {
                        $this->addToRollback($result);
                        // need to retrieve the full record here
                        if ($requireMore) {
                            $result = $this->collection->findOne($criteria, $options);
                            $out = static::cleanRecord($result, $fields, static::DEFAULT_ID_FIELD);
                        } else {
                            $out = [static::DEFAULT_ID_FIELD => $id];
                        }
                    } else {
                        $out = static::fromMongoObjects($result);
                    }

                    if (!empty($relatedInfo)) {
                        $this->updatePostRelations(
                            $this->transactionTable,
                            $out,
                            $relatedInfo,
                            $allowRelatedDelete
                        );
                    }
                } else {
                    $result = $this->collection->updateOne($criteria, $updates, $options);
                    if (1 > $result->getMatchedCount()) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    }
                    $out = [static::DEFAULT_ID_FIELD => $id];
                }
                break;

            case Verbs::DELETE:
                if (!$continue && !$rollback && !$single) {
                    return parent::addToTransaction(null, static::idToMongoId($id));
                }

                // simple delete existing record
                $filter = [static::DEFAULT_ID_FIELD => static::idToMongoId($id)];
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                if ($requireMore || $rollback) {
                    if (!$rollback) {
                        $options['projection'] = static::buildProjection($fields);
                    }
                    if (null === $result = $this->collection->findOneAndDelete($criteria, $options)) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    }
                    if ($rollback) {
                        $this->addToRollback($result);
                        $out = static::cleanRecord($result, $fields, static::DEFAULT_ID_FIELD);
                    } else {
                        $out = static::fromMongoObjects($result);
                    }
                } else {
                    $result = $this->collection->deleteOne($criteria, $options);
                    if (1 > $result->getDeletedCount()) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    }
                    $out = [static::DEFAULT_ID_FIELD => $id];
                }
                break;

            case Verbs::GET:
                if ($continue && !$single) {
                    return parent::addToTransaction(null, static::idToMongoId($id));
                }

                $filter = [static::DEFAULT_ID_FIELD => static::idToMongoId($id)];
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                $options['projection'] = static::buildProjection($fields);
                if (null === $result = $this->collection->findOne($criteria, $options)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                $out = static::fromMongoObjects($result);
                break;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            return null;
        }

        $updates = array_get($extras, 'updates');
        $ssFilters = array_get($extras, 'ss_filters');
        $requireMore = array_get($extras, 'require_more');

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                $result = $this->collection->insertMany($this->batchRecords, ['ordered' => false]);
                $this->batchIds = $result->getInsertedIds();
                if ($requireMore) {
                    $out = $this->findByIds($this->batchIds, $extras);
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::PUT:
                if (empty($updates)) {
                    throw new BadRequestException('Batch operation not supported for update by records.');
                }

                $filter = [static::DEFAULT_ID_FIELD => ['$in' => $this->batchIds]];
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);

                $result = $this->collection->updateMany($criteria, $updates);
                if (0 === $result->getMatchedCount()) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->batchIds) !== $result->getMatchedCount()) {
                    throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                }

                if ($requireMore) {
                    $out = $this->findByIds($this->batchIds, $extras);
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (empty($updates)) {
                    throw new BadRequestException('Batch operation not supported for patch by records.');
                }

                $updates = ['$set' => $updates];

                $filter = [static::DEFAULT_ID_FIELD => ['$in' => $this->batchIds]];
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);

                $result = $this->collection->updateMany($criteria, $updates);
                if (0 === $result->getMatchedCount()) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->batchIds) !== $result->getMatchedCount()) {
                    throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                }

                if ($requireMore) {
                    $out = $this->findByIds($this->batchIds, $extras);
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::DELETE:
                if ($requireMore) {
                    $out = $this->findByIds($this->batchIds, $extras);
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }

                $filter = [static::DEFAULT_ID_FIELD => ['$in' => $this->batchIds]];
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);

                $result = $this->collection->deleteMany($criteria);
                if (0 === $result->getDeletedCount()) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->batchIds) !== $result->getDeletedCount()) {
                    throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                }
                break;

            case Verbs::GET:
                $out = $this->findByIds($this->batchIds, $extras);
                break;

            default:
                break;
        }

        $this->batchIds = [];
        $this->batchRecords = [];

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        if (!empty($this->rollbackRecords)) {
            switch ($this->getAction()) {
                case Verbs::POST:
                    // should be ids here from creation
                    $filter = [static::DEFAULT_ID_FIELD => ['$in' => $this->rollbackRecords]];
                    $this->collection->deleteMany($filter);
                    break;

                case Verbs::PUT:
                case Verbs::PATCH:
                case Verbs::MERGE:
                    foreach ($this->rollbackRecords as $record) {
                        $filter = [static::DEFAULT_ID_FIELD => $record[static::DEFAULT_ID_FIELD]];
                        $this->collection->replaceOne($filter, $record, ['upsert' => true]);
                    }
                    break;
                case Verbs::DELETE:
                    foreach ($this->rollbackRecords as $record) {
                        $this->collection->insertOne($record);
                    }
                    break;

                default:
                    break;
            }

            $this->rollbackRecords = [];
        }

        return true;
    }

    protected function findByIds($ids, $extras)
    {
        $filter = [static::DEFAULT_ID_FIELD => ['$in' => $ids]];
        $ssFilters = array_get($extras, 'ss_filters');
        $criteria = static::buildCriteriaArray($filter, null, $ssFilters);
        $result = $this->runQuery($this->collection->getCollectionName(), $criteria, $extras);
        if (empty($result)) {
            throw new NotFoundException('No records were found using the given identifiers.');
        }

        if (count($this->batchIds) !== count($result)) {
            $out = [];
            $errors = [];
            foreach ($this->batchIds as $index => $id) {
                $found = false;
                foreach ($result as $record) {
                    if ($id == array_get($record, static::DEFAULT_ID_FIELD)) {
                        $out[$index] = $record;
                        $found = true;
                        continue;
                    }
                }
                if (!$found) {
                    $errors[] = $index;
                    $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                }
            }

            if (!empty($errors)) {
                $wrapper = ResourcesWrapper::getWrapper();
                $context = ['error' => $errors, $wrapper => $out];
                throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null, $context);
            }

            return $out;
        }

        return $result;
    }

    protected function runQuery($table, $criteria, $extras)
    {
        $collection = $this->selectTable($table);
        $schema = $this->parent->getSchema()->getResource(DbResourceTypes::TYPE_TABLE, $table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $fields = array_get($extras, ApiOptions::FIELDS);
        $related = array_get($extras, ApiOptions::RELATED);
        /** @type RelationSchema[] $availableRelations */
        $availableRelations = $schema->getRelations(true);
        // see if we need to add anymore fields to select for related retrieval
        if (('*' !== $fields) && !empty($availableRelations) && (!empty($related) || $schema->fetchRequiresRelations)) {
            foreach ($availableRelations as $relation) {
                if (false === array_search($relation->field, $fields)) {
                    $select[] = $relation->field;
                }
            }
        }

        $limit = intval(array_get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(array_get($extras, ApiOptions::OFFSET, 0));
        $sort = static::buildSortArray(array_get($extras, ApiOptions::ORDER));
        $countOnly = Scalar::boolval(array_get($extras, ApiOptions::COUNT_ONLY));
        $includeCount = Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_COUNT, false));
        $maxAllowed = static::getMaxRecordsReturnedLimit();
        $needLimit = false;
        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }

        $options = [];
        if ($offset) {
            $options['skip'] = $offset;
        }
        if ($sort) {
            $options['sort'] = $sort;
        }
        $options['limit'] = $limit;

        $options['projection'] = static::buildProjection($fields);
        $options['typeMap'] = ['root' => 'array', 'document' => 'array'];

        // count total records
        $count = ($countOnly || $includeCount || $needLimit) ? $collection->count($criteria) : 0;

        if ($countOnly) {
            return $count;
        }

        $result = $collection->find($criteria, $options);
        $data = static::cleanRecords($result->toArray());

        if (!empty($data) && (!empty($related) || $schema->fetchRequiresRelations)) {
            if (!empty($availableRelations)) {
                $this->retrieveRelatedRecords($schema, $availableRelations, $related, $data);
            }
        }

        $meta = [];
        if ($includeCount || $needLimit) {
            if ($includeCount || $count > $maxAllowed) {
                $meta['count'] = $count;
            }
            if (($count - $offset) > $limit) {
                $meta['next'] = $offset + $limit;
            }
        }

        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }
}