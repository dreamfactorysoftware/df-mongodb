<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;

class ChangeUsernamePasswordFieldType extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        Schema::table('mongodb_config', function (Blueprint $table) {
            $table->text('username')->change();
            $table->text('password')->change();
        });
    }
}
