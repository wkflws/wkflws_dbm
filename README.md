# wkflws_dbm

This node provides access to Unix style "databases". These are simple key
value stores that are saved on disk. They can be used as a quick way to save
state. In a production system Memcache or Redis is recommended.

## Get

The `get` node will retrieve a value from the store.

### Parameters

The following parameters are available.
| name | required | description |
|-|-|-|
| `filename` | ✅ | a name to give the database file. this is stored in a hard coded path and `.dbm` suffixed. |
| `key` | ✅ | the key to retrieve. |
| `default` | ❌ | The default value to give on cache miss. _Default is null_ |

## Set

The `set` node will write a value to the store.

### Parameters

The following parameters are available.
| name | required | description |
|-|-|-|
| `filename` | ✅ | a name to give the database file. this is stored in a hard coded path and `.dbm` suffixed. |
| `key` | ✅ | the key to retrieve. |
| `value` | ✅ | The value to write to the store. |
| `expiry_secs` | ❌ | The number of seconds the value is valid for. _Default is 300_ |
