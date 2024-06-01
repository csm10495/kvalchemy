# KVAlchemy

[![PyPI version](https://badge.fury.io/py/kvalchemy.svg?dummy=unused)](https://badge.fury.io/py/kvalchemy?dummy=unused)

KVAlchemy is a SQLAlchemy-based key-vault store. It has the ability to get/set values based off a string key, an optional string tag, and an optional expiration time. Additionally it has a built-in ability to memoize function results to the store.

## Example

```
from kvalchemy import KVAlchemy

# Setup. Supports any available sqlalchemy backend.
k = KVAlchemy('sqlite://')

# Set/Get
k.set("hello", "world")
assert k.get("hello") == "world"

# Default values
assert k.get("hello again", "default") == "default"

# memoize example
import time

@k.memoize()
def func():
    time.sleep(1)

func() # Will sleep
func() # Won't sleep

func.cache_clear()
func() # Will sleep

# proxy example
proxy = k.get_proxy('pizza')
proxy.set('pie')
assert proxy.get() == 'pie'
proxy.delete()
assert proxy.get('default') == 'default'
```

## Installation

On Python 3.8 or later:

```
pip install kvalchemy
```

## Versioning

```
Note that the database format is stable across the same patch version.

For example: Version 0.0.1 will be fully compatible with all releases in the 0.0.X family.
Though Version 0.1.0 may not be directly compatible without a manual data migration.

Make sure to pin the version family you want: kvalachemy<X.(Y+1).0
```

## Testing
KVAlchemy is tested across multiple database backends including MySQL, Postgres, SQLite, and Oracle.

[![Run tests and release](https://github.com/csm10495/kvalchemy/actions/workflows/test_and_release.yml/badge.svg)](https://github.com/csm10495/kvalchemy/actions/workflows/test_and_release.yml)

## More Documentation

For more documentation visit: https://csm10495.github.io/kvalchemy
