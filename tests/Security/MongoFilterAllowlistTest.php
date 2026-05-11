<?php

namespace DreamFactory\Core\MongoDb\Tests\Security;

use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\MongoDb\Resources\Table;
use PHPUnit\Framework\TestCase;

/**
 * Security: MongoDB filter operators that execute server-side JavaScript
 * must be rejected when present in caller-supplied filters.
 */
class MongoFilterAllowlistTest extends TestCase
{
    /**
     * @dataProvider unsafeFilterProvider
     */
    public function testRejectsUnsafeOperator(array $filter): void
    {
        $this->expectException(BadRequestException::class);
        Table::assertSafeMongoFilter($filter);
    }

    public static function unsafeFilterProvider(): array
    {
        return [
            'where top-level'     => [['$where' => 'this.password.match(/a/)']],
            'where capitalized'   => [['$Where' => 'this.x']],
            'function operator'   => [['$function' => ['body' => '...', 'args' => [], 'lang' => 'js']]],
            'accumulator op'      => [['field' => ['$accumulator' => []]]],
            'expr nested'         => [['$expr' => ['$gt' => ['$field', 1]]]],
            'mapReduce key'       => [['mapReduce' => 'col']],
            'where nested in $or' => [['$or' => [['name' => 'x'], ['$where' => 'this.password']]]],
            'where in $and'       => [['$and' => [['$where' => 'this.x']]]],
        ];
    }

    /**
     * @dataProvider safeFilterProvider
     */
    public function testAcceptsSafeFilter(array $filter): void
    {
        Table::assertSafeMongoFilter($filter);
        $this->assertTrue(true);
    }

    public static function safeFilterProvider(): array
    {
        return [
            'simple equality'    => [['name' => 'alice']],
            'comparison'         => [['age' => ['$gt' => 18]]],
            'in operator'        => [['status' => ['$in' => ['A', 'B']]]],
            'nested $and ok'     => [['$and' => [['x' => 1], ['y' => 2]]]],
            'nested $or ok'      => [['$or' => [['x' => 1], ['y' => 2]]]],
            'regex'              => [['name' => ['$regex' => '^a']]],
            'empty'              => [[]],
        ];
    }
}
