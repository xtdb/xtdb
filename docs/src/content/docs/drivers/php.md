---
title: Using XTDB from PHP
---

In PHP, you can talk to a running XTDB node using the [ext-pq](https://pecl.php.net/package/pq) (PECL pq) PostgreSQL driver, taking advantage of XTDB's PostgreSQL wire-compatibility.

## Install

To install ext-pq, use PECL:

```bash
pecl install pq
```

Then enable it in your `php.ini`:

```ini
extension=pq.so
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

```php
<?php

// Connect to XTDB using ext-pq (PECL pq) driver
use pq\Connection;

try {
    // Connect to XTDB
    $connection = new Connection("host=localhost port=5432 dbname=xtdb user=xtdb password=");

    // Insert records using XTDB's RECORDS syntax
    $insert_query = "INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}";
    $connection->exec($insert_query);

    // Query the table and print results
    $result = $connection->exec("SELECT * FROM users");

    echo "Users:\n";

    while ($row = $result->fetchRow(\pq\Result::FETCH_ASSOC)) {
        echo "  * " . $row['_id'] . ": " . $row['name'] . "\n";
    }

    echo "\n✓ XTDB connection successful\n";

} catch (\pq\Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}

/* Output:

Users:
  * jms: James
  * joe: Joe

✓ XTDB connection successful
*/

?>
```

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
