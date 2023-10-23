<?php

namespace DreamFactory\Core\MongoDb\Enums;

/**
 * Binary data types for MongoDB
 * https://www.mongodb.com/docs/manual/reference/bson-types/#binary-data
 */
class BinaryDataTypes
{
    const GENERIC = 0;
    const FUNCTION_DATA = 1;
    const BINARY_OLD = 2;
    const UUID_OLD = 3;
    const UUID = 4;
    const MD5 = 5;
    const ENCRYPTED_BSON_VALUE = 6;
    const COMPRESSED_TIME_SERIES_DATA = 7;
    const CUSTOM_DATA = 128;
}
