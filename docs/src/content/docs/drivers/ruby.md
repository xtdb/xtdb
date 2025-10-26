---
title: Using XTDB from Ruby
---

In Ruby, you can talk to a running XTDB node using the [Sequel](https://github.com/jeremyevans/sequel) gem or the native [pg](https://github.com/ged/ruby-pg) gem, taking advantage of XTDB's PostgreSQL wire-compatibility.

## Install

To install the Sequel gem, add it to your Gemfile:

```ruby
gem 'sequel'
gem 'pg'  # PostgreSQL adapter
```

Or install directly:

```bash
gem install sequel pg
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

```ruby
require 'sequel'

DB = Sequel.connect("xtdb://xtdb:5432/xtdb")

DB << "INSERT INTO ruby_users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}"

puts "Users:"
DB["SELECT _id, name FROM ruby_users"].each do |row|
  puts "  * #{row[:_id]}: #{row[:name]}"
end

puts "\n✓ XTDB connection successful"

# Output:
#
# Users:
#   * alice: Alice
#   * bob: Bob
#
# ✓ XTDB connection successful
```

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
