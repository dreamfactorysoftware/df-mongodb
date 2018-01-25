<?php

use DreamFactory\Core\Enums\Verbs;

use DreamFactory\Core\File\Testing\FileServiceTestCase;

class MongoDbTest extends FileServiceTestCase
{
    protected static $staged = false;

    protected $serviceId = 'gridfs';



    public function stage()
    {

        parent::stage();
        if (!$this->serviceExists('gridfs')) {
            \DreamFactory\Core\Models\Service::create(
                [
                    'name'        => $this->serviceId,
                    'label'       => 'MongoDB Database Gridfs',
                    'description' => 'MongoDB database for testing Gridfs',
                    'is_active'   => true,
                    'type'        => 'gridfs',
                    'config'      => [
                        'host'     => env('MONGODB_HOST'),
                        'port'     => env('MONGODB_PORT'),
                        'dsn'      => env('MONGODB_DSN'),
                        'username' => env('MONGODB_USER'),
                        'password' => env('MONGODB_PASSWORD'),
                        'database' => env('MONGODB_DB')
                    ]
                ]
            );
        }

    }



    /************************************************
     * Testing POST
     ************************************************/

    /**
     * @inheritdoc
     */
    public function testPOSTFolder()
    {
        return parent::testPOSTFolder();
    }

    /**
     * @inheritdoc
     */
    public function testPOSTFolderWithCheckExist()
    {
        return parent::testPOSTFolderWithCheckExist();
    }

    /**
     * @inheritdoc
     */
    public function testPOSTFolderAndFile()
    {
        return parent::testPOSTFolderAndFile();
    }

    /**
     * @inheritdoc
     */
    public function testPOSTZipFileFromUrl()
    {
        return parent::testPOSTZipFileFromUrl();
    }

    /**
     * @inheritdoc
     */
    public function testPOSTZipFileFromUrlWithExtractAndClean()
    {
        $rs = $this->makeRequest(
            Verbs::POST,
            static::FOLDER_1 . '/f2/',
            ['url' => 'http://' . static::LOCAL_HOST . '/testfiles.zip', 'extract' => 'true', 'clean' => 'true']
        );
        $content = json_encode($rs->getContent(), JSON_UNESCAPED_SLASHES);
        $this->assertEquals('{"name":"' .
            static::FOLDER_1 .
            '/f2","path":"' .
            static::FOLDER_1 .
            '/f2/"}',
            $content);
    }

    /************************************************
     * Testing GET
     ************************************************/

    /**
     * @inheritdoc
     */
    public function testGETFolderAndFile()
    {
        return parent::testGETFolderAndFile();
    }

    /**
     * @inheritdoc
     */
    public function testGETFolders()
    {
        return parent::testGETFolders();
    }

    /**
     * @inheritdoc
     */
    public function testGETFolderIncludeProperties()
    {
        return parent::testGETFolderIncludeProperties();
    }

    /************************************************
     * Testing DELETE
     ************************************************/

    /**
     * @inheritdoc
     */
    public function testDELETEfile()
    {
        return parent::testDELETEfile();
    }

    /**
     * @inheritdoc
     */
    public function testDELETEZipFile()
    {
        return parent::testDELETEZipFile();
    }

    /**
     * @inheritdoc
     */
    public function testDELETEFolderByForce()
    {
        return parent::testDELETEFolderByForce();
    }

    /**
     * @inheritdoc
     */
    public function testDELETEfolder()
    {
        return parent::testDELETEfolder();
    }

    /**
     * @inheritdoc
     */
    public function testDELETEFoldersWithPayload()
    {
        $this->addFolder(["resource" => [["name" => static::FOLDER_1, "type" => "folder"]]]);
        $this->addFolder(["resource" => [["name" => static::FOLDER_2, "type" => "folder"]]]);

        $payload =
            '{"resource":[{"name":"' .
            static::FOLDER_1 .
            '/"},{"name":"' .
            static::FOLDER_2 .
            '/"}]}';

        $rs = $this->makeRequest(Verbs::DELETE, null, ['force' => true], $payload);

        $expected = '{"resource":[]}';

        $this->assertEquals($expected, json_encode($rs->getContent(), JSON_UNESCAPED_SLASHES));
    }

    /**
     * @inheritdoc
     */
    public function testDeleteFolderWithPathOnUrl()
    {
        return parent::testDeleteFolderWithPathOnUrl();
    }

    /**
     * @inheritdoc
     */
    public function testDELETEFolderWithPayload()
    {
        $this->addFolder(["resource" => [["name" => static::FOLDER_4, "type" => "folder"]]]);

        $payload = '{"resource":[{"name":"' . static::FOLDER_4 . '"}]}';

        $rs = $this->makeRequest(Verbs::DELETE, null, ['force' => true], $payload);
        $expected = '{"resource":[]}';

        $this->assertEquals($expected, json_encode($rs->getContent(), JSON_UNESCAPED_SLASHES));
    }

}